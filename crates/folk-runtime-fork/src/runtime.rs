//! `ForkRuntime`: spawns PHP workers via prefork master + `SCM_RIGHTS`.

use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use folk_core::runtime::{Runtime, WorkerHandle};
use folk_runtime_pipe::socket::create_socketpair;
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use crate::handle::ForkWorkerHandle;
use crate::master::PreforkMaster;

/// Configuration for the fork runtime.
#[derive(Clone)]
pub struct ForkConfig {
    pub php: String,
    pub script: String,
    pub boot_timeout: std::time::Duration,
}

/// Fork-based runtime. Holds a shared reference to the prefork master.
pub struct ForkRuntime {
    // Stored for master recycling (future use).
    _config: ForkConfig,
    master: Mutex<Option<PreforkMaster>>,
}

impl ForkRuntime {
    /// Create a new fork runtime and spawn the prefork master.
    pub async fn new(config: ForkConfig) -> Result<Arc<Self>> {
        let master = PreforkMaster::spawn(&config.php, &config.script, config.boot_timeout).await?;

        Ok(Arc::new(Self {
            _config: config,
            master: Mutex::new(Some(master)),
        }))
    }
}

#[async_trait]
#[allow(unsafe_code)]
impl Runtime for ForkRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        let (task_master, task_child) = create_socketpair()?;
        let (ctrl_master, ctrl_child) = create_socketpair()?;

        let child_pid = {
            let mut master_guard = self.master.lock().await;
            let master = master_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("prefork master not running"))?;
            master.fork_worker(&task_child, &ctrl_child).await?
        };

        // Child now owns copies of these FDs; drop ours.
        drop(task_child);
        drop(ctrl_child);

        // Convert master FDs to tokio UnixStreams.
        let task_stream = {
            let std_sock =
                unsafe { std::os::unix::net::UnixStream::from_raw_fd(task_master.into_raw_fd()) };
            std_sock.set_nonblocking(true)?;
            UnixStream::from_std(std_sock)?
        };
        let ctrl_stream = {
            let std_sock =
                unsafe { std::os::unix::net::UnixStream::from_raw_fd(ctrl_master.into_raw_fd()) };
            std_sock.set_nonblocking(true)?;
            UnixStream::from_std(std_sock)?
        };

        Ok(Box::new(ForkWorkerHandle::new(
            child_pid,
            task_stream,
            ctrl_stream,
        )))
    }
}
