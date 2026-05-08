//! Prefork master management: spawn the master PHP process and issue fork commands.

use std::os::unix::io::{AsRawFd, OwnedFd};

use anyhow::{Context, Result, bail};
use folk_protocol::{FrameCodec, RpcMessage};
use futures_util::{SinkExt, StreamExt};
use rmpv::Value;
use tokio::net::UnixStream;
use tokio_util::codec::Framed;
use tracing::{debug, info};

use crate::scm_rights::send_fds;

/// Manages the prefork master PHP process.
pub struct PreforkMaster {
    control: Framed<UnixStream, FrameCodec>,
    pid: u32,
    /// Socket for sending FDs to the master via `SCM_RIGHTS`.
    fork_socket: UnixStream,
}

impl PreforkMaster {
    /// Spawn the PHP prefork master (`FOLK_RUNTIME=fork-master`).
    /// Waits for `control.fork-ready` within `boot_timeout`.
    pub async fn spawn(php: &str, script: &str, boot_timeout: std::time::Duration) -> Result<Self> {
        // Re-use PipeRuntime's spawn_worker for the initial master process.
        let spawned =
            folk_runtime_pipe::spawn::spawn_worker(php, script).context("spawn prefork master")?;
        let mut control = Framed::new(spawned.control_master, FrameCodec::new());
        let pid = spawned.child.id().unwrap_or(0);

        // Wait for fork-ready
        let ready = tokio::time::timeout(boot_timeout, control.next())
            .await
            .context("boot timeout")?
            .context("EOF before fork-ready")?
            .context("decode fork-ready")?;

        match ready {
            RpcMessage::Notify { ref method, .. } if method == "control.fork-ready" => {
                info!(pid, "prefork master ready");
            },
            other => bail!("expected control.fork-ready, got {other:?}"),
        }

        Ok(Self {
            control,
            pid,
            fork_socket: spawned.task_master,
        })
    }

    /// Send the master a fork command + two socket FDs (task + control for the new child).
    /// Returns the child's PID from the master's reply.
    #[allow(unsafe_code)]
    pub async fn fork_worker(&mut self, task_child: &OwnedFd, ctrl_child: &OwnedFd) -> Result<u32> {
        // Send FDs via SCM_RIGHTS over fork_socket
        unsafe {
            send_fds(
                self.fork_socket.as_raw_fd(),
                &[task_child.as_raw_fd(), ctrl_child.as_raw_fd()],
            )?;
        }

        // Send fork command on control channel
        self.control
            .send(RpcMessage::notify("fork.spawn", Value::Nil))
            .await?;

        // Receive child PID from master
        let reply = tokio::time::timeout(std::time::Duration::from_secs(10), self.control.next())
            .await
            .context("fork reply timeout")?
            .context("EOF from master")?
            .context("decode fork reply")?;

        match reply {
            RpcMessage::Notify { method, params } if method == "fork.spawned" => {
                #[allow(clippy::cast_possible_truncation)]
                let pid = params
                    .as_map()
                    .and_then(|m| m.iter().find(|(k, _)| k.as_str() == Some("pid")))
                    .and_then(|(_, v)| v.as_u64())
                    .context("missing pid in fork.spawned")? as u32;
                debug!(child_pid = pid, "child forked");
                Ok(pid)
            },
            other => bail!("unexpected reply to fork.spawn: {other:?}"),
        }
    }

    /// Shut down the prefork master.
    #[allow(unsafe_code, clippy::cast_possible_wrap)]
    pub async fn shutdown(&mut self) -> Result<()> {
        let _ = self
            .control
            .send(RpcMessage::notify("control.shutdown", Value::Nil))
            .await;
        unsafe {
            libc::kill(self.pid as libc::pid_t, libc::SIGTERM);
        }
        Ok(())
    }
}
