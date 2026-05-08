//! `PipeWorkerHandle`: wraps two Framed Unix streams for task+control.

use anyhow::Result;
use async_trait::async_trait;
use folk_core::runtime::WorkerHandle;
use folk_protocol::{FrameCodec, RpcMessage};
use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio::process::Child;
use tokio_util::codec::Framed;

pub struct PipeWorkerHandle {
    child: Child,
    task: Framed<UnixStream, FrameCodec>,
    control: Framed<UnixStream, FrameCodec>,
}

impl PipeWorkerHandle {
    pub fn new(child: Child, task_stream: UnixStream, control_stream: UnixStream) -> Self {
        Self {
            child,
            task: Framed::new(task_stream, FrameCodec::new()),
            control: Framed::new(control_stream, FrameCodec::new()),
        }
    }
}

#[async_trait]
impl WorkerHandle for PipeWorkerHandle {
    fn pid(&self) -> u32 {
        self.child.id().unwrap_or(0)
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

    #[allow(unsafe_code)]
    async fn terminate(&mut self) -> Result<()> {
        let pid = self.child.id().unwrap_or(0);

        // Send SIGTERM (tokio's start_kill sends SIGKILL, not SIGTERM)
        // SAFETY: kill is a well-defined syscall with a valid pid.
        #[allow(clippy::cast_possible_wrap)]
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }

        // Wait up to 5 seconds for clean exit
        let grace = tokio::time::Duration::from_secs(5);
        let exited = tokio::time::timeout(grace, self.child.wait()).await;

        if let Ok(Ok(_)) = exited {
            tracing::debug!(pid, "worker exited cleanly after SIGTERM");
        } else {
            tracing::warn!(pid, "worker did not exit after SIGTERM; sending SIGKILL");
            // SAFETY: kill with SIGKILL on a valid pid.
            #[allow(clippy::cast_possible_wrap)]
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
            let _ = self.child.wait().await;
        }

        Ok(())
    }
}
