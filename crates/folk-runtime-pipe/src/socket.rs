//! Safe wrapper around `socketpair(2)`.
//!
//! Creates a connected pair of Unix stream sockets. Both ends are marked
//! `FD_CLOEXEC` after creation; the child's end must have this flag cleared
//! inside `pre_exec` (see `spawn.rs`).

use std::os::unix::io::{FromRawFd, OwnedFd};

use anyhow::{Result, bail};

/// Returns `(master_fd, child_fd)` as `OwnedFd` values.
///
/// The `master_fd` stays in the Rust process. The `child_fd` is dup2'd
/// into the child's descriptor slot inside `pre_exec`.
#[allow(unsafe_code)]
pub fn create_socketpair() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds: [libc::c_int; 2] = [-1, -1];

    // SAFETY: socketpair is a well-defined syscall; we check the return code.
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };

    if rc != 0 {
        bail!("socketpair failed: {}", std::io::Error::last_os_error());
    }

    // Set FD_CLOEXEC on both ends (macOS doesn't support SOCK_CLOEXEC).
    for &fd in &fds {
        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    }

    // SAFETY: fds are valid file descriptors returned by socketpair.
    let master = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let child = unsafe { OwnedFd::from_raw_fd(fds[1]) };

    Ok((master, child))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;

    #[test]
    fn creates_connected_pair() {
        let (master, child) = create_socketpair().unwrap();
        assert!(master.as_raw_fd() >= 0);
        assert!(child.as_raw_fd() >= 0);
        assert_ne!(master.as_raw_fd(), child.as_raw_fd());
    }
}
