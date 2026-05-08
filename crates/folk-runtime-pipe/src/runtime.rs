//! `PipeRuntime`: spawns PHP workers via execve + Unix socketpairs.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use folk_core::runtime::{Runtime, WorkerHandle};
use tracing::info;

use crate::handle::PipeWorkerHandle;
use crate::spawn::spawn_worker;

#[derive(Clone)]
pub struct PipeConfig {
    pub php: String,
    pub script: String,
}

pub struct PipeRuntime {
    config: Arc<PipeConfig>,
}

impl PipeRuntime {
    pub fn new(config: PipeConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl Runtime for PipeRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        info!(php = %self.config.php, script = %self.config.script, "spawning worker");
        let spawned = spawn_worker(&self.config.php, &self.config.script)?;
        Ok(Box::new(PipeWorkerHandle::new(
            spawned.child,
            spawned.task_master,
            spawned.control_master,
        )))
    }
}
