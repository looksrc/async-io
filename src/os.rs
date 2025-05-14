//! Platform-specific functionality.
//! 
//! 平台特定的功能。

#[cfg(unix)]
pub mod unix;

#[cfg(any(
    target_vendor = "apple",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly",
))]
pub mod kqueue;

#[cfg(windows)]
pub mod windows;
