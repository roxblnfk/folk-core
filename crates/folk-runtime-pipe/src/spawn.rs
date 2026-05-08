//! PHP worker process spawn with FD inheritance.

use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};

use anyhow::Result;
use tokio::net::UnixStream;
use tokio::process::Command;
use tracing::debug;

use crate::socket::create_socketpair;

/// File descriptor numbers in the child process.
pub const TASK_FD: libc::c_int = 3;
pub const CONTROL_FD: libc::c_int = 4;

/// What we get back from a successful spawn.
pub struct SpawnedWorker {
    pub child: tokio::process::Child,
    pub task_master: UnixStream,
    pub control_master: UnixStream,
}

/// Spawn a PHP worker process.
///
/// Sets `FOLK_RUNTIME=pipe`, `FOLK_TASK_FD=3`, `FOLK_CONTROL_FD=4` in the
/// environment. The child receives a connected Unix socket on each of FD 3
/// and FD 4.
///
/// # Safety
///
/// Calls `pre_exec` which runs `libc::dup2` and `libc::close` in the
/// child after fork but before exec. This is safe here because:
/// - We only use async-signal-safe functions (`dup2`, `close`, `fcntl`).
/// - We do not allocate or use Rust data structures in the callback.
#[allow(unsafe_code)]
pub fn spawn_worker(php: &str, script: &str) -> Result<SpawnedWorker> {
    let (task_master_fd, task_child_fd) = create_socketpair()?;
    let (ctrl_master_fd, ctrl_child_fd) = create_socketpair()?;

    let task_child_raw = task_child_fd.as_raw_fd();
    let ctrl_child_raw = ctrl_child_fd.as_raw_fd();

    let mut cmd = Command::new(php);
    cmd.arg(script)
        .env("FOLK_RUNTIME", "pipe")
        .env("FOLK_TASK_FD", TASK_FD.to_string())
        .env("FOLK_CONTROL_FD", CONTROL_FD.to_string())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    // SAFETY: we only call async-signal-safe libc functions and use only
    // captured primitive integers (no heap allocation, no Rust drop).
    unsafe {
        cmd.pre_exec(move || {
            if libc::dup2(task_child_raw, TASK_FD) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::dup2(ctrl_child_raw, CONTROL_FD) < 0 {
                return Err(std::io::Error::last_os_error());
            }

            if task_child_raw != TASK_FD {
                libc::close(task_child_raw);
            }
            if ctrl_child_raw != CONTROL_FD {
                libc::close(ctrl_child_raw);
            }

            libc::fcntl(TASK_FD, libc::F_SETFD, 0);
            libc::fcntl(CONTROL_FD, libc::F_SETFD, 0);

            Ok(())
        });
    }

    let child = cmd.spawn()?;

    drop(task_child_fd);
    drop(ctrl_child_fd);

    let task_master = {
        let std_sock =
            unsafe { std::os::unix::net::UnixStream::from_raw_fd(task_master_fd.into_raw_fd()) };
        std_sock.set_nonblocking(true)?;
        UnixStream::from_std(std_sock)?
    };

    let control_master = {
        let std_sock =
            unsafe { std::os::unix::net::UnixStream::from_raw_fd(ctrl_master_fd.into_raw_fd()) };
        std_sock.set_nonblocking(true)?;
        UnixStream::from_std(std_sock)?
    };

    debug!(pid = ?child.id(), "worker spawned");

    Ok(SpawnedWorker {
        child,
        task_master,
        control_master,
    })
}
