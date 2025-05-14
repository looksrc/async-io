use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::panic;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use async_lock::OnceCell;
use concurrent_queue::ConcurrentQueue;
use futures_lite::ready;
use polling::{Event, Events, Poller};
use slab::Slab;

// Choose the proper implementation of `Registration` based on the target platform.
cfg_if::cfg_if! {
    if #[cfg(windows)] {
        mod windows;
        pub use windows::Registration;
    } else if #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))] {
        mod kqueue;
        pub use kqueue::Registration;
    } else if #[cfg(unix)] {
        mod unix;
        pub use unix::Registration;
    } else {
        compile_error!("unsupported platform");
    }
}

#[cfg(not(target_os = "espidf"))]
const TIMER_QUEUE_SIZE: usize = 1000;

/// ESP-IDF - being an embedded OS - does not need so many timers
/// and this saves ~ 20K RAM which is a lot for an MCU with RAM < 400K
#[cfg(target_os = "espidf")]
const TIMER_QUEUE_SIZE: usize = 100;

const READ: usize = 0;
const WRITE: usize = 1;

/// The reactor.
///
/// 发生器结构体
///
/// There is only one global instance of this type, accessible by [`Reactor::get()`].
///
/// 单例模式，通过[`Reactor::get()`]访问。
pub(crate) struct Reactor {
    /// Portable bindings to epoll/kqueue/event ports/IOCP.
    ///
    /// This is where I/O is polled, producing I/O events.
    pub(crate) poller: Poller,

    /// Ticker bumped before polling.
    ///
    /// This is useful for checking what is the current "round" of `ReactorLock::react()` when
    /// synchronizing things in `Source::readable()` and `Source::writable()`. Both of those
    /// methods must make sure they don't receive stale I/O events - they only accept events from a
    /// fresh "round" of `ReactorLock::react()`.
    ///
    /// 发生器时钟，随着轮询递增。
    ///
    /// 作用：
    /// - 在`Source::readable()`和`Source::writable()`中做同步时，校验`ReactorLock::react()`的当前轮次。
    /// - 上述两种方法必须确保他们不会收到老的IO事件，他们只会从最新一轮`ReactorLock::react()中收取事件。
    ticker: AtomicUsize,

    /// Registered sources.
    ///
    /// 注册的事件源，包含目标文件描述符、对应的唤醒器等信息。
    sources: Mutex<Slab<Arc<Source>>>,

    /// Temporary storage for I/O events when polling the reactor.
    ///
    /// 临时存储轮询到的IO事件。
    ///
    /// Holding a lock on this event list implies the exclusive right to poll I/O.
    ///
    /// 持有此锁者，可以独占的轮询IO。
    events: Mutex<Events>,

    /// An ordered map of registered timers.
    ///
    /// 一个已注册的定时器的排序字典。
    ///
    /// Timers are in the order in which they fire. The `usize` in this type is a timer ID used to
    /// distinguish timers that fire at the same time. The `Waker` represents the task awaiting the
    /// timer.
    ///
    /// 定时器以发射时间排序，`usize`用以区分同一时间发生的不同定时器。`Waker`代表定时器的唤醒器。
    timers: Mutex<BTreeMap<(Instant, usize), Waker>>,

    /// A queue of timer operations (insert and remove).
    ///
    /// 定时器操作序列。
    ///
    /// When inserting or removing a timer, we don't process it immediately - we just push it into
    /// this queue. Timers actually get processed when the queue fills up or the reactor is polled.
    ///
    /// 当插入或移除一个定时器时，不会立即执行，而是将操作插入缓此队列。
    /// 只有在队列满了或者发生器被轮询时，才会真正的执行操作。
    timer_ops: ConcurrentQueue<TimerOp>,
}

impl Reactor {
    /// Returns a reference to the reactor.
    ///
    /// 读取或初始化发生器单例。
    pub(crate) fn get() -> &'static Reactor {
        static REACTOR: OnceCell<Reactor> = OnceCell::new();

        REACTOR.get_or_init_blocking(|| {
            crate::driver::init();
            Reactor {
                poller: Poller::new().expect("cannot initialize I/O event notification"),
                ticker: AtomicUsize::new(0),
                sources: Mutex::new(Slab::new()),
                events: Mutex::new(Events::new()),
                timers: Mutex::new(BTreeMap::new()),
                timer_ops: ConcurrentQueue::bounded(TIMER_QUEUE_SIZE),
            }
        })
    }

    /// Returns the current ticker.
    ///
    /// 获取轮询时钟。
    pub(crate) fn ticker(&self) -> usize {
        self.ticker.load(Ordering::SeqCst)
    }

    /// Registers an I/O source in the reactor.
    ///
    /// 通过fd创建IO事件源，注册到注册表、IO框架。
    ///
    /// 步骤：
    /// - 创建：fd + key + Direction × 2
    /// - 入表：Arc<Source>
    /// - 提交IO框架：
    ///   - 感兴趣事件：fd
    ///   - token: key
    ///   - 如果添加失败，清理掉事件源。
    /// - 返回：Arc<Source>
    pub(crate) fn insert_io(&self, raw: Registration) -> io::Result<Arc<Source>> {
        // Create an I/O source for this file descriptor.
        let source = {
            let mut sources = self.sources.lock().unwrap();
            let key = sources.vacant_entry().key();
            let source = Arc::new(Source {
                registration: raw,
                key,
                state: Default::default(),
            });
            sources.insert(source.clone());
            source
        };

        // Register the file descriptor.
        if let Err(err) = source.registration.add(&self.poller, source.key) {
            let mut sources = self.sources.lock().unwrap();
            sources.remove(source.key);
            return Err(err);
        }

        Ok(source)
    }

    /// Deregisters an I/O source from the reactor.
    ///
    /// 从发生器和IO框架移除事件源。
    pub(crate) fn remove_io(&self, source: &Source) -> io::Result<()> {
        let mut sources = self.sources.lock().unwrap();
        sources.remove(source.key);
        source.registration.delete(&self.poller)
    }

    /// Registers a timer in the reactor.
    ///
    /// 将定时任务(Instant,Waker)，注册到发生器的定时器表，返回定时器ID。
    ///
    /// Returns the inserted timer's ID.
    ///
    /// 步骤：
    /// - 定义一个ID生成器初始为1，递增生成ID;
    /// - 将插入操作写入操作队列;
    /// - 驱动定时器操作队列处理所有操作，注意操作时需要先锁住定时器表;
    /// - 通知因Reactor::react()而阻塞的线程;
    pub(crate) fn insert_timer(&self, when: Instant, waker: &Waker) -> usize {
        // Generate a new timer ID.
        static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);
        let id = ID_GENERATOR.fetch_add(1, Ordering::Relaxed);

        // Push an insert operation.
        while self
            .timer_ops
            .push(TimerOp::Insert(when, id, waker.clone()))
            .is_err()
        {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.lock().unwrap();
            self.process_timer_ops(&mut timers);
        }

        // Notify that a timer has been inserted.
        self.notify();

        id
    }

    /// Deregisters a timer from the reactor.
    ///
    /// 从发生器清除定时发射任务。
    ///
    /// 步骤：
    /// - 删除操作写入操作队列。
    /// - 驱动操作队列执行操作。需要先锁定定时器表。
    pub(crate) fn remove_timer(&self, when: Instant, id: usize) {
        // Push a remove operation.
        while self.timer_ops.push(TimerOp::Remove(when, id)).is_err() {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.lock().unwrap();
            self.process_timer_ops(&mut timers);
        }
    }

    /// Notifies the thread blocked on the reactor.
    ///
    /// 通知因Reactor::react()而阻塞的线程;
    pub(crate) fn notify(&self) {
        self.poller.notify().expect("failed to notify reactor");
    }

    /// Locks the reactor, potentially blocking if the lock is held by another thread.
    ///
    /// 阻塞锁定发生器。实际的锁在events字段上。
    ///
    /// 步骤：
    /// - 锁定events字段。字段表示轮询到的IO事件。
    /// - 返回{Reactor,Events}
    pub(crate) fn lock(&self) -> ReactorLock<'_> {
        let reactor = self;
        let events = self.events.lock().unwrap();
        ReactorLock { reactor, events }
    }

    /// Attempts to lock the reactor.
    ///
    /// 非阻塞尝试锁定发生器。底层尝试锁定Events。
    pub(crate) fn try_lock(&self) -> Option<ReactorLock<'_>> {
        self.events.try_lock().ok().map(|events| {
            let reactor = self;
            ReactorLock { reactor, events }
        })
    }

    /// Processes ready timers and extends the list of wakers to wake.
    ///
    /// 处理就绪的定时器，插入待唤醒的唤醒器列表。
    ///
    /// Returns the duration until the next timer before this method was called.
    ///
    /// 返回下一次调用此方法的时间间隔。由下一个定时器的触发时间决定。
    ///
    /// 1.从定时器表获取已到期定时器列表：
    /// - 先处理时间操作队列
    /// - 将增序排列的定时器字典，按(now+1ns)为界，砍下后半段未到期的定时器，留下已到到期的定时器。
    /// - 将未到期的定时器句柄，替换出原字典中的已到期的定时器句柄。
    ///
    /// 2.计算下一次轮询定时器的间隔：
    /// - 如果已到期定时器列表为空，则取下一个最近的定时器，计算间隔。
    /// - 否则，间隔为0，立即发射定时器。
    ///
    /// 3.将已到期的定时器唤醒器，追加到待发射的唤醒器列表。
    fn process_timers(&self, wakers: &mut Vec<Waker>) -> Option<Duration> {
        let span = tracing::trace_span!("process_timers");
        let _enter = span.enter();

        let mut timers = self.timers.lock().unwrap();
        self.process_timer_ops(&mut timers);

        let now = Instant::now();

        // Split timers into ready and pending timers.
        //
        // Careful to split just *after* `now`, so that a timer set for exactly `now` is considered
        // ready.
        let pending = timers.split_off(&(now + Duration::from_nanos(1), 0));
        let ready = mem::replace(&mut *timers, pending);

        // Calculate the duration until the next event.
        let dur = if ready.is_empty() {
            // Duration until the next timer.
            timers
                .keys()
                .next()
                .map(|(when, _)| when.saturating_duration_since(now))
        } else {
            // Timers are about to fire right now.
            Some(Duration::from_secs(0))
        };

        // Drop the lock before waking.
        drop(timers);

        // Add wakers to the list.
        tracing::trace!("{} ready wakers", ready.len());

        for (_, waker) in ready {
            wakers.push(waker);
        }

        dur
    }

    /// Processes queued timer operations.
    ///
    /// 执行定时发射任务操作队列。由于使用BTreeMap，插入和删除都是自动重排序。
    fn process_timer_ops(&self, timers: &mut MutexGuard<'_, BTreeMap<(Instant, usize), Waker>>) {
        // Process only as much as fits into the queue, or else this loop could in theory run
        // forever.
        self.timer_ops
            .try_iter()
            .take(self.timer_ops.capacity().unwrap())
            .for_each(|op| match op {
                TimerOp::Insert(when, id, waker) => {
                    timers.insert((when, id), waker);
                }
                TimerOp::Remove(when, id) => {
                    timers.remove(&(when, id));
                }
            });
    }
}

/// A lock on the reactor.
///
/// 发生器锁。锁字段为Reactor::events。
pub(crate) struct ReactorLock<'a> {
    reactor: &'a Reactor,
    events: MutexGuard<'a, Events>,
}

impl ReactorLock<'_> {
    /// Processes new events, blocking until the first event or the timeout.
    ///
    /// 处理新事件，阻塞直到产生首个事件或超时。
    pub(crate) fn react(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let span = tracing::trace_span!("react");
        let _enter = span.enter();

        let mut wakers = Vec::new();

        // Process ready timers.
        // 处理就绪的定时器，返回下一次处理间隔。回传已到期定时器的唤醒器列表。
        let next_timer = self.reactor.process_timers(&mut wakers);

        // compute the timeout for blocking on I/O events.
        // 计算发生器阻塞时间。
        //
        // 逻辑：
        // - 所谓发生器阻塞时间，即距离下一次需要执行发生器的最短间隔时间。
        // - 取法：在下一次定时器时间，和发生器超时时间，取有值的一方，或取较小者。
        let timeout = match (next_timer, timeout) {
            (None, None) => None,
            (Some(t), None) | (None, Some(t)) => Some(t),
            (Some(a), Some(b)) => Some(a.min(b)),
        };

        // Bump the ticker before polling I/O.
        // 递增发生器时钟。如果计数溢出则从头开始。
        let tick = self
            .reactor
            .ticker
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);

        // 清理历史事件。
        self.events.clear();

        // Block on I/O events.
        // 阻塞轮询底层IO框架，并设定超时时间。(因为还要处理定时器或超时，不可能一直等待)
        //
        // 1.轮询到的事件数为0：
        // - 超时时间如果不为0，说明等待一段时间了，得赶紧轮询一次定时器。
        // - 否则返回OK
        //
        // 2.如果轮询到了就绪事件
        // - 锁定注册事件源。
        // - 迭代就绪事件，每个事件操作如下：
        //   - 根据事件key，查询出注册事件源。
        //   - 依据就绪事件内容，将对应事件源中的唤醒器写入待唤醒列表，同时递增发生器时钟。
        //   - 对于事件源没匹配上的感兴趣事件，比如只匹配了读没匹配上写，则事件重新注册到IO框架。
        //
        // 3.轮询事件时报错：
        // - ErrKind::Interupted：正常返回OK
        // - 其它错误：返回Err(err)
        //
        // 最终：
        // - 对待唤醒列表中的所有Waker执行唤醒操作。
        let res = match self.reactor.poller.wait(&mut self.events, timeout) {
            // No I/O events occurred.
            Ok(0) => {
                if timeout != Some(Duration::from_secs(0)) {
                    // The non-zero timeout was hit so fire ready timers.
                    self.reactor.process_timers(&mut wakers);
                }
                Ok(())
            }

            // At least one I/O event occurred.
            Ok(_) => {
                // Iterate over sources in the event list.
                let sources = self.reactor.sources.lock().unwrap();

                for ev in self.events.iter() {
                    // Check if there is a source in the table with this key.
                    if let Some(source) = sources.get(ev.key) {
                        let mut state = source.state.lock().unwrap();

                        // Collect wakers if a writability event was emitted.
                        for &(dir, emitted) in &[(WRITE, ev.writable), (READ, ev.readable)] {
                            if emitted {
                                state[dir].tick = tick;
                                state[dir].drain_into(&mut wakers);
                            }
                        }

                        // Re-register if there are still writers or readers. This can happen if
                        // e.g. we were previously interested in both readability and writability,
                        // but only one of them was emitted.
                        if !state[READ].is_empty() || !state[WRITE].is_empty() {
                            // Create the event that we are interested in.
                            let event = {
                                let mut event = Event::none(source.key);
                                event.readable = !state[READ].is_empty();
                                event.writable = !state[WRITE].is_empty();
                                event
                            };

                            // Register interest in this event.
                            source.registration.modify(&self.reactor.poller, event)?;
                        }
                    }
                }

                Ok(())
            }

            // The syscall was interrupted.
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(()),

            // An actual error occureed.
            Err(err) => Err(err),
        };

        // Wake up ready tasks.
        tracing::trace!("{} ready wakers", wakers.len());
        for waker in wakers {
            // Don't let a panicking waker blow everything up.
            panic::catch_unwind(|| waker.wake()).ok();
        }

        res
    }
}

/// A single timer operation.
///
/// 定时器操作，分为插入和移除。
enum TimerOp {
    Insert(Instant, usize, Waker),
    Remove(Instant, usize),
}

/// A registered source of I/O events.
///
/// 一个注册的IO事件源。
#[derive(Debug)]
pub(crate) struct Source {
    /// This source's registration into the reactor.
    ///
    /// 事件源主体，通常是文件描述符。
    registration: Registration,

    /// The key of this source obtained during registration.
    ///
    /// 事件源在`Reactor::sources`表中的键，提交给IO框架时用作token。
    key: usize,

    /// Inner state with registered wakers.
    ///
    /// 事件源对应读和写两种事件，各自对应的所有唤醒器。
    state: Mutex<[Direction; 2]>,
}

/// A read or write direction.
///
/// 事件源的读或写状态。包含发生器时钟和事件对应的唤醒器。
#[derive(Debug, Default)]
struct Direction {
    /// Last reactor tick that delivered an event.
    ///
    /// 上次投递事件时所在时钟。
    tick: usize,

    /// Ticks remembered by `Async::poll_readable()` or `Async::poll_writable()`.
    ///
    /// 由`Async::poll_readable()` 或 `Async::poll_writable()`记录的发生器时钟。
    ticks: Option<(usize, usize)>,

    /// Waker stored by `Async::poll_readable()` or `Async::poll_writable()`.
    ///
    /// 直接调用事件源的可读可写轮询函数时，插入的唤醒器。(为什么只有一个？)
    ///
    /// 由`Async::poll_readable()` or `Async::poll_writable()`存入的唤醒器。
    waker: Option<Waker>,

    /// Wakers of tasks waiting for the next event.
    ///
    /// Registered by `Async::readable()` and `Async::writable()`.
    ///
    /// 等待某个事件源是否可读可写时，插入的唤醒器。
    ///
    /// 由`Async::readable()` and `Async::writable()`注册。
    wakers: Slab<Option<Waker>>,
}

impl Direction {
    /// Returns `true` if there are no wakers interested in this direction.
    ///
    /// 判断事件方向是否有唤醒器。
    fn is_empty(&self) -> bool {
        self.waker.is_none() && self.wakers.iter().all(|(_, opt)| opt.is_none())
    }

    /// Moves all wakers into a `Vec`.
    ///
    /// 将此事件方向的唤醒器全部排入待唤醒列表，用于集中执行唤醒。
    fn drain_into(&mut self, dst: &mut Vec<Waker>) {
        if let Some(w) = self.waker.take() {
            dst.push(w);
        }
        for (_, opt) in self.wakers.iter_mut() {
            if let Some(w) = opt.take() {
                dst.push(w);
            }
        }
    }
}

impl Source {
    /// Polls the I/O source for readability.
    ///
    /// 轮询一次事件源是否通畅可读。
    pub(crate) fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_ready(READ, cx)
    }

    /// Polls the I/O source for writability.
    ///
    /// 轮询一次事件源是否通畅可写。
    pub(crate) fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_ready(WRITE, cx)
    }

    /// Registers a waker from `poll_readable()` or `poll_writable()`.
    ///
    /// If a different waker is already registered, it gets replaced and woken.
    ///
    /// 轮询IO的可读写性，向读写方向注册唤醒器。
    ///
    /// 如果存在旧的唤醒器，将其换出并执行唤醒。
    fn poll_ready(&self, dir: usize, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();

        // Check if the reactor has delivered an event.
        // 判定事件源是否被发生器投递了事件给自己。
        //
        // 逻辑：
        // 1.发生器投递事件时，同时将tick设为投递时所在的发生器时钟。即事件是在tick时被投递的。
        // 2.事件源向IO框架注册新事件时，或替换了老的唤醒器时，ticks设为(发生器当前时钟,事件源记录时钟)。
        // 3.当再次判断事件源的时钟，即不等于投递时间，也不等于。
        //  - 第一个不等：投递时间与新加Waker时间(注册时间)或更换Wake时间不能相等，必须大于。
        //  - 第二个不等：投递时间与事件源创建时间不等，必须大于。
        if let Some((a, b)) = state[dir].ticks {
            // If `state[dir].tick` has changed to a value other than the old reactor tick,
            // that means a newer reactor tick has delivered an event.
            if state[dir].tick != a && state[dir].tick != b {
                state[dir].ticks = None;
                return Poll::Ready(Ok(()));
            }
        }

        // 有无唤醒器。
        let was_empty = state[dir].is_empty();

        // Register the current task's waker.
        //
        // 1.处理旧唤醒器：
        // - 新旧指向同一个任务，则将旧的写回，返回Pending。
        // - 新旧指向不同的任务，则先执行一次旧唤醒器。
        //
        // 2.写入新唤醒器：
        // - 如果没有旧唤醒器，或新旧唤醒器不指向同一个任务，则写入新唤醒器。
        // - 更新ticks：第一个数记录发生器当前时钟，第二个数为事件的旧时钟。
        //
        // 3.没有唤醒器时，注册感兴趣事件到IO框架：
        // - 如果没有唤醒器，说明这是任务的首次轮询，需要将感兴趣事件注册到IO框架，
        if let Some(w) = state[dir].waker.take() {
            if w.will_wake(cx.waker()) {
                state[dir].waker = Some(w);
                return Poll::Pending;
            }
            // Wake the previous waker because it's going to get replaced.
            panic::catch_unwind(|| w.wake()).ok();
        }
        state[dir].waker = Some(cx.waker().clone());
        state[dir].ticks = Some((Reactor::get().ticker(), state[dir].tick));

        // Update interest in this I/O handle.
        // 更新轮询事件
        //
        if was_empty {
            // Create the event that we are interested in.
            let event = {
                let mut event = Event::none(self.key);
                event.readable = !state[READ].is_empty();
                event.writable = !state[WRITE].is_empty();
                event
            };

            // Register interest in it.
            // 感兴趣事件注册到IO框架。
            self.registration.modify(&Reactor::get().poller, event)?;
        }

        Poll::Pending
    }

    /// Waits until the I/O source is readable.
    ///
    /// 等待事件主体变得可读。引用版。
    pub(crate) fn readable<T>(handle: &crate::Async<T>) -> Readable<'_, T> {
        Readable(Self::ready(handle, READ))
    }

    /// Waits until the I/O source is readable.
    ///
    /// 等待事件主体变得可读。引用计数版。
    pub(crate) fn readable_owned<T>(handle: Arc<crate::Async<T>>) -> ReadableOwned<T> {
        ReadableOwned(Self::ready(handle, READ))
    }

    /// Waits until the I/O source is writable.
    ///
    /// 等待事件主体变得可写。引用版。
    pub(crate) fn writable<T>(handle: &crate::Async<T>) -> Writable<'_, T> {
        Writable(Self::ready(handle, WRITE))
    }

    /// Waits until the I/O source is writable.
    ///
    /// 等待事件主体变得可写。引用计数版。
    pub(crate) fn writable_owned<T>(handle: Arc<crate::Async<T>>) -> WritableOwned<T> {
        WritableOwned(Self::ready(handle, WRITE))
    }

    /// Waits until the I/O source is readable or writable.
    ///
    /// 等待某个事件主体变为可读或可写。
    fn ready<H: Borrow<crate::Async<T>> + Clone, T>(handle: H, dir: usize) -> Ready<H, T> {
        Ready {
            handle,
            dir,
            ticks: None,
            index: None,
            _capture: PhantomData,
        }
    }
}

/// Future for [`Async::readable`](crate::Async::readable).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Readable<'a, T>(Ready<&'a crate::Async<T>, T>);

impl<T> Future for Readable<'_, T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        tracing::trace!(fd = ?self.0.handle.source.registration, "readable");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for Readable<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Readable").finish()
    }
}

/// Future for [`Async::readable_owned`](crate::Async::readable_owned).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadableOwned<T>(Ready<Arc<crate::Async<T>>, T>);

impl<T> Future for ReadableOwned<T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        tracing::trace!(fd = ?self.0.handle.source.registration, "readable_owned");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for ReadableOwned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadableOwned").finish()
    }
}

/// Future for [`Async::writable`](crate::Async::writable).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Writable<'a, T>(Ready<&'a crate::Async<T>, T>);

impl<T> Future for Writable<'_, T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        tracing::trace!(fd = ?self.0.handle.source.registration, "writable");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for Writable<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writable").finish()
    }
}

/// Future for [`Async::writable_owned`](crate::Async::writable_owned).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WritableOwned<T>(Ready<Arc<crate::Async<T>>, T>);

impl<T> Future for WritableOwned<T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        tracing::trace!(fd = ?self.0.handle.source.registration, "writable_owned");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for WritableOwned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WritableOwned").finish()
    }
}

/// 用于校验操作是否就绪(是否有事件投递)的异步操作。
struct Ready<H: Borrow<crate::Async<T>>, T> {
    /// 可借用为Async的IO对象。
    handle: H,
    /// 当前异步操作的操作种类：读或写。
    dir: usize,
    /// 两个时钟：
    /// - 0.当前操作首次被轮询的时间(发生器时钟)。
    /// - 1.当前操作对应的事件源上次投递事件时间(发生器时钟)。
    ticks: Option<(usize, usize)>,
    /// 当前操作所属任务的唤醒器在本事件源对应的所有唤醒器中的索引。
    /// 一个事件源可能同时被多个任务中的异步操作使用，因此对应多个唤醒器。
    index: Option<usize>,
    _capture: PhantomData<fn() -> T>,
}

impl<H: Borrow<crate::Async<T>>, T> Unpin for Ready<H, T> {}

impl<H: Borrow<crate::Async<T>> + Clone, T> Future for Ready<H, T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            ref handle,
            dir,
            ticks,
            index,
            ..
        } = &mut *self;

        // 取当前异步操作对应的事件源
        let mut state = handle.borrow().source.state.lock().unwrap();

        // Check if the reactor has delivered an event.
        // 校验发生器是否投递了当前操作对应的事件(读或写)
        // 事件源每投递新事件会刷新自身时钟，因此事件源的时钟：
        // 1.既不等于当前操作的首次被轮询时间
        // 2.也不等于上次收到并处理投递事件的时间
        // 则说明，事件源中的这个是新投递的事件还未被任何人处理过。
        //
        if let Some((a, b)) = *ticks {
            // If `state[dir].tick` has changed to a value other than the old reactor tick,
            // that means a newer reactor tick has delivered an event.
            if state[*dir].tick != a && state[*dir].tick != b {
                return Poll::Ready(Ok(()));
            }
        }

        // 记录事件源的原始状态，即是否有对应的待唤醒操作，
        // 如果为空，说明是首次轮询，要将感兴趣事件注册到发生器中。
        let was_empty = state[*dir].is_empty();

        // Register the current task's waker.
        // 将当前任务的唤醒器更新到事件源。并返回所在位置。
        // 1.如果Ready原先记录过位置，则说明本任务的唤醒器注册到过事件源，替换之。
        // 2.否则，说明是任务首次被轮询，在事件源中找一个空闲位置，插入之。
        //
        // 对于情况2，即任务首次被轮询，需要更新两个时钟
        // 时钟0：记录操作首次被轮询时时间，即发生器当前时间。
        // 时钟1：记录上次收到并处理投递事件的时间。
        // 此两个时间通过与时间源最新投递时间比较来判定是否有未处理过的新投递事件。
        let i = match *index {
            Some(i) => i,
            None => {
                let i = state[*dir].wakers.insert(None);
                *index = Some(i);
                *ticks = Some((Reactor::get().ticker(), state[*dir].tick));
                i
            }
        };
        state[*dir].wakers[i] = Some(cx.waker().clone());

        // Update interest in this I/O handle.
        // 如果事件源对应的本操作方向空空如也，说明事件未注册，注册之。
        // - 已注册过的事件继续保留
        // - 未注册过的事件追加进去。
        //
        // 如何判定对哪种事件感兴趣：
        // 因为前面已经将任务的唤醒器注册到事件源，说明正在等待事件。
        // 因此，通过判断是否存在唤醒器来确定是否关注事件。
        //
        // 最终，将感兴趣事件更新到发生器持有的最底层的IO框架。
        if was_empty {
            // Create the event that we are interested in.
            let event = {
                let mut event = Event::none(handle.borrow().source.key);
                event.readable = !state[READ].is_empty();
                event.writable = !state[WRITE].is_empty();
                event
            };

            // Indicate that we are interested in this event.
            handle
                .borrow()
                .source
                .registration
                .modify(&Reactor::get().poller, event)?;
        }

        Poll::Pending
    }
}

impl<H: Borrow<crate::Async<T>>, T> Drop for Ready<H, T> {
    fn drop(&mut self) {
        // Remove our waker when dropped.
        if let Some(key) = self.index {
            let mut state = self.handle.borrow().source.state.lock().unwrap();
            let wakers = &mut state[self.dir].wakers;
            if wakers.contains(key) {
                wakers.remove(key);
            }
        }
    }
}
