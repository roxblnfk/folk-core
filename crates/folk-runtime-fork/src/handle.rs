//! `ForkWorkerHandle`: wraps two Framed Unix streams for a forked child.
//!
//! Similar to `PipeWorkerHandle` but the child is identified by PID (not
//! a `tokio::process::Child`), since the PHP master owns the forked process.

use anyhow::Result;
use async_trait::async_trait;
use folk_core::runtime::WorkerHandle;
use folk_protocol::{FrameCodec, RpcMessage};
use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio_util::codec::Framed;

pub struct ForkWorkerHandle {
    pid: u32,
    task: Framed<UnixStream, FrameCodec>,
    control: Framed<UnixStream, FrameCodec>,
}

impl ForkWorkerHandle {
    pub fn new(pid: u32, task_stream: UnixStream, control_stream: UnixStream) -> Self {
        Self {
            pid,
            task: Framed::new(task_stream, FrameCodec::new()),
            control: Framed::new(control_stream, FrameCodec::new()),
        }
    }
}

#[async_trait]
impl WorkerHandle for ForkWorkerHandle {
    fn pid(&self) -> u32 {
        self.pid
    }

    async fn send_task(&mut self, msg: RpcMessage) -> Result<()> {
        self.task.send(msg).await?;
        Ok(())
    }

    async fn recv_task(&mut self) -> Result<Option<RpcMessage>> {
        match self.task.next().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    async fn send_control(&mut self, msg: RpcMessage) -> Result<()> {
        self.control.send(msg).await?;
        Ok(())
    }

    async fn recv_control(&mut self) -> Result<Option<RpcMessage>> {
        match self.control.next().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    #[allow(unsafe_code, clippy::cast_possible_wrap)]
    async fn terminate(&mut self) -> Result<()> {
        // Send SIGTERM
        unsafe {
            libc::kill(self.pid as libc::pid_t, libc::SIGTERM);
        }

        // Wait 5 seconds, then SIGKILL
        let grace = tokio::time::Duration::from_secs(5);
        tokio::time::sleep(grace).await;

        // Check if still alive, send SIGKILL
        let alive = unsafe { libc::kill(self.pid as libc::pid_t, 0) } == 0;
        if alive {
            tracing::warn!(
                pid = self.pid,
                "worker did not exit after SIGTERM; sending SIGKILL"
            );
            unsafe {
                libc::kill(self.pid as libc::pid_t, libc::SIGKILL);
            }
        }

        Ok(())
    }
}
