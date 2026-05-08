//! Send and receive file descriptors over a Unix socket via `SCM_RIGHTS`.
//!
//! Uses raw `libc::sendmsg` / `libc::recvmsg` with ancillary data.
//! This is inherently unsafe; all callers must ensure the socket is valid
//! and the FDs to send are open.

use std::os::unix::io::RawFd;

use anyhow::{Result, bail};

/// Send `fds` over `socket_fd` as ancillary data (`SCM_RIGHTS`).
///
/// # Safety
/// `socket_fd` must be a valid Unix stream socket.
/// All `fds` must be valid open file descriptors.
#[allow(unsafe_code, clippy::cast_possible_truncation)]
pub unsafe fn send_fds(socket_fd: RawFd, fds: &[RawFd]) -> Result<()> {
    let fd_bytes_len = std::mem::size_of_val(fds);

    let cmsg_space = unsafe { libc::CMSG_SPACE(fd_bytes_len as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let dummy = [0u8; 1];
    let iov = libc::iovec {
        iov_base: dummy.as_ptr() as *mut libc::c_void,
        iov_len: 1,
    };

    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: (&raw const iov).cast_mut(),
        msg_iovlen: 1,
        msg_control: cmsg_buf.as_mut_ptr().cast(),
        msg_controllen: cmsg_space as _,
        msg_flags: 0,
    };

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(fd_bytes_len as u32) as _;

        std::ptr::copy_nonoverlapping(
            fds.as_ptr().cast::<u8>(),
            libc::CMSG_DATA(cmsg),
            fd_bytes_len,
        );
    }

    msg.msg_controllen = unsafe { libc::CMSG_SPACE(fd_bytes_len as u32) } as _;

    let ret = unsafe { libc::sendmsg(socket_fd, &msg, 0) };
    if ret < 0 {
        bail!("sendmsg failed: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Receive up to `max_fds` file descriptors from `socket_fd`.
///
/// # Safety
/// `socket_fd` must be a valid connected Unix socket.
/// The caller owns the returned FDs and must close them.
#[allow(unsafe_code, clippy::cast_possible_truncation)]
pub unsafe fn recv_fds(socket_fd: RawFd, max_fds: usize) -> Result<Vec<RawFd>> {
    let fd_size = max_fds * std::mem::size_of::<RawFd>();
    let cmsg_space = unsafe { libc::CMSG_SPACE(fd_size as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];
    let mut data = [0u8; 1];

    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &raw mut iov,
        msg_iovlen: 1,
        msg_control: cmsg_buf.as_mut_ptr().cast(),
        msg_controllen: cmsg_space as _,
        msg_flags: 0,
    };

    let ret = unsafe { libc::recvmsg(socket_fd, &mut msg, 0) };
    if ret < 0 {
        bail!("recvmsg failed: {}", std::io::Error::last_os_error());
    }

    let mut fds = Vec::new();
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if !cmsg.is_null()
            && (*cmsg).cmsg_level == libc::SOL_SOCKET
            && (*cmsg).cmsg_type == libc::SCM_RIGHTS
        {
            let data_len = (*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
            let n = data_len / std::mem::size_of::<RawFd>();
            let fd_ptr = libc::CMSG_DATA(cmsg).cast::<RawFd>();
            for i in 0..n {
                fds.push(*fd_ptr.add(i));
            }
        }
    }

    Ok(fds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;

    #[test]
    #[allow(unsafe_code)]
    fn send_and_recv_fd_via_scm_rights() {
        let (master, child) = folk_runtime_pipe::socket::create_socketpair().unwrap();

        // Create a pipe to get a valid FD to send
        let mut pipe_fds = [0i32; 2];
        unsafe {
            libc::pipe(pipe_fds.as_mut_ptr());
        }

        unsafe {
            send_fds(master.as_raw_fd(), &[pipe_fds[0]]).unwrap();
        }

        let received = unsafe { recv_fds(child.as_raw_fd(), 1).unwrap() };

        assert_eq!(received.len(), 1);
        let flags = unsafe { libc::fcntl(received[0], libc::F_GETFD) };
        assert!(flags >= 0, "received FD should be valid");

        // Clean up
        unsafe {
            libc::close(pipe_fds[0]);
            libc::close(pipe_fds[1]);
            libc::close(received[0]);
        }
    }
}
