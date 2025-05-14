use std::cell::{Cell, RefCell};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Waker;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use async_lock::OnceCell;
use futures_lite::pin;
use parking::Parker;

use crate::reactor::Reactor;

/// Number of currently active `block_on()` invocations.<br>
/// 当前活动的`block_on()`调用的数量。
static BLOCK_ON_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Unparker for the "async-io" thread.<br>
/// 获取"async-io"线程的唤醒器。如果线程不存在则创建。
/// 
/// 唤醒器存在全局变量UNPARKER：(单例)
/// - 如果UNPARKER未初始化，则新建"async-io"线程、新建唤醒对，初始化为Unparker。Parker传给`async-io`。
/// - 如果UNPARKER已初始化，则直接返回"async-io"线程的唤醒器。
fn unparker() -> &'static parking::Unparker {
    static UNPARKER: OnceCell<parking::Unparker> = OnceCell::new();

    UNPARKER.get_or_init_blocking(|| {
        let (parker, unparker) = parking::pair();

        // Spawn a helper thread driving the reactor.
        // 此线程用于循环驱动发生器。
        //
        // Note that this thread is not exactly necessary, it's only here to help push things
        // forward if there are no `Parker`s around or if `Parker`s are just idling and never
        // parking.
        // 
        // 如果不存在`block_on`时，全靠它驱动发生器。
        // `block_on`也会在空闲时驱动发生器，如果没有`block_on`，就全靠`async-io`
        thread::Builder::new()
            .name("async-io".to_string())
            .spawn(move || main_loop(parker))
            .expect("cannot spawn async-io thread");

        unparker
    })
}

/// Initializes the "async-io" thread.<br>
/// 初始化"async-io"线程。即初始化UNPARKER并创建"async-io"线程。这是一个单例操作。
pub(crate) fn init() {
    let _ = unparker();
}

/// The main loop for the "async-io" thread.<br>
/// `async-io`线程的主循环。
fn main_loop(parker: parking::Parker) {
    let span = tracing::trace_span!("async_io::main_loop");
    let _enter = span.enter();

    // The last observed reactor tick.
    // 上一个处理过的发生器时钟。
    let mut last_tick = 0;
    // Number of sleeps since this thread has called `react()`.
    // 自从此线程调用`react()`后的休眠次数。
    let mut sleeps = 0u64;

    loop {
        // 获取最新发生器时钟。
        let tick = Reactor::get().ticker();

        // 获取的时钟与上次处理的时钟相同，则：
        // - 休眠次数小于10情况下，仅通过try_lock尝试获取锁，获取不到也没关系。
        // - 如果休眠了10次或更多，说明已经很久没获取到发生器锁了，使用自旋直到获取。
        //
        // 一旦获取到发生器锁：
        // - 执行一次发生器。
        // - 再一次获取最新的发生器时钟。
        // - 休眠次数清零。
        //
        // 与上次时钟不同时
        // - 只更新游标
        if last_tick == tick {
            let reactor_lock = if sleeps >= 10 {
                // If no new ticks have occurred for a while, stop sleeping and spinning in
                // this loop and just block on the reactor lock.
                Some(Reactor::get().lock())
            } else {
                Reactor::get().try_lock()
            };

            if let Some(mut reactor_lock) = reactor_lock {
                tracing::trace!("waiting on I/O");
                reactor_lock.react(None).ok();
                last_tick = Reactor::get().ticker();
                sleeps = 0;
            }
        } else {
            last_tick = tick;
        }

        // 
        // 如果存在`block_on`，执行线程休眠：
        // - 根据休眠次数，获取对应的休眠时长。
        // - 根数时长利用`park_timeout`执行休眠，
        // - 休眠时，如果被主动唤醒，则执行发生器，超时唤醒则休眠计数加一，进入下一次循环。
        //
        // 根据休眠期间是否被唤醒：
        // - 是，获取最新的发生器时钟(因为在线程外发生器被执行过)，休眠次数清零
        // - 否，休眠次数递增1.
        if BLOCK_ON_COUNT.load(Ordering::SeqCst) > 0 {
            // Exponential backoff from 50us to 10ms.
            let delay_us = [50, 75, 100, 250, 500, 750, 1000, 2500, 5000]
                .get(sleeps as usize)
                .unwrap_or(&10_000);

            tracing::trace!("sleeping for {} us", delay_us);
            if parker.park_timeout(Duration::from_micros(*delay_us)) {
                tracing::trace!("notified");

                // If notified before timeout, reset the last tick and the sleep counter.
                last_tick = Reactor::get().ticker();
                sleeps = 0;
            } else {
                sleeps += 1;
            }
        }
    }
}

/// Blocks the current thread on a future, processing I/O events when idle.<br>
/// 阻塞当前线程执行一个异步任务，空闲时处理IO事件。
///
/// # Examples
///
/// ```
/// use async_io::Timer;
/// use std::time::Duration;
///
/// async_io::block_on(async {
///     // This timer will likely be processed by the current
///     // thread rather than the fallback "async-io" thread.
///     Timer::after(Duration::from_millis(1)).await;
/// });
/// ```
pub fn block_on<T>(future: impl Future<Output = T>) -> T {
    // 跟踪范围
    let span = tracing::trace_span!("async_io::block_on");
    let _enter = span.enter();

    // Increment `BLOCK_ON_COUNT` so that the "async-io" thread becomes less aggressive.
    // 跟踪`block_on`线程数量。
    // - 当存在`block_on`时，"async-io"不必过于积极，在适当的时候休眠。因为`block_on`也会驱动发生器。
    // - 如果没有block_on存在，则"async-io"处于积极模式，永不会休眠。(比如只存在孵化任务的时候)
    BLOCK_ON_COUNT.fetch_add(1, Ordering::SeqCst);

    // Make sure to decrement `BLOCK_ON_COUNT` at the end and wake the "async-io" thread.
    // `block_on`线程数量递减守卫，当block_on返回时：
    // - 计数减一
    // - 唤醒async-io线程，执行一次发生器。
    // 
    let _guard = CallOnDrop(|| {
        BLOCK_ON_COUNT.fetch_sub(1, Ordering::SeqCst);
        unparker().unpark();
    });

    // Creates a parker and an associated waker that unparks it.
    // 创建一个休眠句柄和一个包含了唤醒句柄的的任务唤醒器。
    //
    // Arc io_blocked：
    // - 本线程是否正在被IO阻塞的标记。是则为true。
    // - 唤醒句柄和当前线程都持有此标记。
    fn parker_and_waker() -> (Parker, Waker, Arc<AtomicBool>) {
        // Parker and unparker for notifying the current thread.
        let (p, u) = parking::pair();

        // This boolean is set to `true` when the current thread is blocked on I/O.
        let io_blocked = Arc::new(AtomicBool::new(false));

        // Prepare the waker.
        let waker = BlockOnWaker::create(io_blocked.clone(), u);

        (p, waker, io_blocked)
    }

    // 线程本地变量
    thread_local! {
        /// Cached parker and waker for efficiency.
        /// 缓存了线程索自身要使用的休眠句柄、唤醒器、io_blocked标记。
        static CACHE: RefCell<(Parker, Waker, Arc<AtomicBool>)> = RefCell::new(parker_and_waker());

        /// Indicates that the current thread is polling I/O, but not necessarily blocked on it.
        /// 标记当前线程正在执行轮询IO的操作，但是没必要阻塞它。
        /// 主要作用：此状态共享给Waker，Waker依据此状态发送唤醒通知。（同样的还有个io_block)
        static IO_POLLING: Cell<bool> = const { Cell::new(false) };
    }

    /// block_on异步任务的唤醒器
    /// - io_blocked：线程因IO而阻塞的标记。
    /// - unparkder：线程的唤醒句柄
    /// 
    /// 要想实现唤醒器，需要2个步骤：
    /// - BlockWaker实现Wake接口
    /// - 通过Waker::from(BlockWaker)创建标准的Waker。
    /// 
    /// io_blocked：
    /// - 作用：block_on线程将自己状态暴露给Waker，Waker依据状态执行唤醒。
    /// - 设置：由block_on将自身状态设置给io_blocked。
    /// - 使用：由Waker在执行唤醒时，依据此状态。
    struct BlockOnWaker {
        io_blocked: Arc<AtomicBool>,
        unparker: parking::Unparker,
    }

    impl BlockOnWaker {
        fn create(io_blocked: Arc<AtomicBool>, unparker: parking::Unparker) -> Waker {
            Waker::from(Arc::new(BlockOnWaker {
                io_blocked,
                unparker,
            }))
        }
    }

    impl std::task::Wake for BlockOnWaker {
        /// 唤醒block_on线程。
        /// 
        /// 如果是重复唤醒(上次的唤醒还没有被Parker消耗)，则：
        /// 如果线程因IO而阻塞，且线程没有在轮询IO，则发生器执行通知。
        fn wake_by_ref(self: &Arc<Self>) {
            if self.unparker.unpark() {
                // Check if waking from another thread and if currently blocked on I/O.
                if !IO_POLLING.with(Cell::get) && self.io_blocked.load(Ordering::SeqCst) {
                    Reactor::get().notify();
                }
            }
        }

        fn wake(self: Arc<Self>) {
            self.wake_by_ref()
        }
    }

    // 每个block_on都需要一组(Parker,Waker,io_blocked)支持，
    // - 通过可变借用线程缓存的。可以提升效率。
    // - 通过自己现场创建一组。
    //
    // 如果从缓存借用失败：
    // - 说明此为一个block_on中嵌套的block_on，缓存已经被根block_on借走。
    // - 解决方法：是自己新建一套(Parker,Waker,io_blocked)。
    CACHE.with(|cache| {
        // Try grabbing the cached parker and waker.
        let tmp_cached;
        let tmp_fresh;
        let (p, waker, io_blocked) = match cache.try_borrow_mut() {
            Ok(cache) => {
                // Use the cached parker and waker.
                tmp_cached = cache;
                &*tmp_cached
            }
            Err(_) => {
                // Looks like this is a recursive `block_on()` call.
                // Create a fresh parker and waker.
                tmp_fresh = parker_and_waker();
                &tmp_fresh
            }
        };

        // 锁定future使之无法移动。
        pin!(future);

        // 创建异步轮询上下文，附加唤醒器。
        let cx = &mut Context::from_waker(waker);

        // 开始轮询任务。
        loop {
            // Poll the future.
            // 立即轮询任务，轮询成功后，消耗所有线程唤醒通知，block_on返回。
            if let Poll::Ready(t) = future.as_mut().poll(cx) {
                // Ensure the cached parker is reset to the unnotified state for future block_on calls,
                // in case this future called wake and then immediately returned Poll::Ready.
                // 通过一次休眠并立即唤醒，消耗掉所有的唤醒通知，避免后续的休眠失败。
                p.park_timeout(Duration::from_secs(0));
                tracing::trace!("completed");
                return t;
            }

            // 轮询未成功

            // Check if a notification was received.
            // 通过休眠0秒的返回值，判定是否有线程唤醒通知。
            // 
            // 如果有唤醒通知：
            // - 设置IO_POLLING为true，标记当前线程正在调用发生器阻塞轮询IO事件（直到轮询到事件)。并添加恢复为false的守卫。
            // - 执行发生器，处理可用的IO事件。
            // - 跳过剩余代码，开始新一轮循环，去执行轮询。
            if p.park_timeout(Duration::from_secs(0)) {
                tracing::trace!("notified");

                // Try grabbing a lock on the reactor to process I/O events.
                if let Some(mut reactor_lock) = Reactor::get().try_lock() {
                    // First let wakers know this parker is processing I/O events.
                    IO_POLLING.with(|io| io.set(true));
                    let _guard = CallOnDrop(|| {
                        IO_POLLING.with(|io| io.set(false));
                    });

                    // Process available I/O events.
                    reactor_lock.react(Some(Duration::from_secs(0))).ok();
                }
                continue;
            }

            // 未收到唤醒通知。

            // Try grabbing a lock on the reactor to wait on I/O.
            // 
            // 如果获取到了发生器锁（独占)：
            // - 此时，由锁定者负责自旋催动发生器轮询所有的IO(不仅是自己的)，直到收到自己的线程唤醒通知。
            // - IO_POLLING和io_block此时都被标记为true，既在轮询IO事件，又在用loop自旋阻塞block_on。
            // 
            // 自旋超过时间限制：
            // - 努力了超过500微秒还没有收到唤醒通知，表明你的努力都用来唤醒了其它线程。
            // - 因此，释放发生器锁，给其它线程处理IO事件的机会。
            // - 然后唤醒async-io线程,弥补没有其它因此推动IO轮询的情况。（比如所有人都在Parker)
            // - 最后，让当前线程进入休眠，等待唤醒通知。
            //
            // 如果没有获取到发生器锁：
            // - 说明发生器被其它人占用了，由它负责轮询IO。自己只要休眠等通知就行了。
            if let Some(mut reactor_lock) = Reactor::get().try_lock() {
                // Record the instant at which the lock was grabbed.
                let start = Instant::now();

                loop {
                    // First let wakers know this parker is blocked on I/O.
                    IO_POLLING.with(|io| io.set(true));
                    io_blocked.store(true, Ordering::SeqCst);
                    let _guard = CallOnDrop(|| {
                        IO_POLLING.with(|io| io.set(false));
                        io_blocked.store(false, Ordering::SeqCst);
                    });

                    // Check if a notification has been received before `io_blocked` was updated
                    // because in that case the reactor won't receive a wakeup.
                    if p.park_timeout(Duration::from_secs(0)) {
                        tracing::trace!("notified");
                        break;
                    }

                    // Wait for I/O events.
                    tracing::trace!("waiting on I/O");
                    reactor_lock.react(None).ok();

                    // Check if a notification has been received.
                    if p.park_timeout(Duration::from_secs(0)) {
                        tracing::trace!("notified");
                        break;
                    }

                    // Check if this thread been handling I/O events for a long time.
                    if start.elapsed() > Duration::from_micros(500) {
                        tracing::trace!("stops hogging the reactor");

                        // This thread is clearly processing I/O events for some other threads
                        // because it didn't get a notification yet. It's best to stop hogging the
                        // reactor and give other threads a chance to process I/O events for
                        // themselves.
                        drop(reactor_lock);

                        // Unpark the "async-io" thread in case no other thread is ready to start
                        // processing I/O events. This way we prevent a potential latency spike.
                        unparker().unpark();

                        // Wait for a notification.
                        p.park();
                        break;
                    }
                }
            } else {
                // Wait for an actual notification.
                tracing::trace!("sleep until notification");
                p.park();
            }
        }
    })
}

/// Runs a closure when dropped.
struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
