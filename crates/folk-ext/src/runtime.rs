//! Extension runtime: pre-connected channel-based workers.
//!
//! Creates N channel pairs before the server starts. Each spawn() returns
//! a handle connected to the next channel pair. The PHP side (main process
//! or forked children) takes the rx end via bridge::init_worker_state().

use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use folk_core::config::WorkersConfig;
use folk_core::runtime::{Runtime, WorkerHandle};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::bridge;

static NEXT_WORKER_ID: AtomicU32 = AtomicU32::new(1);

/// A pre-created channel pair for one worker.
pub struct WorkerChannels {
    pub task_tx: mpsc::Sender<bridge::TaskRequest>,
    pub task_rx: mpsc::Receiver<bridge::TaskRequest>,
    pub ready_tx: oneshot::Sender<()>,
    pub ready_rx: oneshot::Receiver<()>,
}

/// Runtime with N pre-connected channel pairs.
pub struct ExtensionRuntime {
    #[allow(dead_code)]
    config: WorkersConfig,
    /// Pre-created channel pairs. Taken one at a time by spawn().
    channels: std::sync::Mutex<Vec<(mpsc::Sender<bridge::TaskRequest>, oneshot::Receiver<()>)>>,
}

impl ExtensionRuntime {
    /// Create a runtime with pre-connected channels.
    /// `tx_sides` contains (task_tx, ready_rx) for each worker.
    /// The rx sides are stored globally for PHP processes to pick up.
    pub fn new(
        config: WorkersConfig,
        tx_sides: Vec<(mpsc::Sender<bridge::TaskRequest>, oneshot::Receiver<()>)>,
    ) -> Self {
        Self {
            config,
            channels: std::sync::Mutex::new(tx_sides),
        }
    }
}

#[async_trait]
impl Runtime for ExtensionRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);

        let (task_tx, ready_rx) = self.channels.lock().unwrap().pop().ok_or_else(|| {
            anyhow::anyhow!("no more pre-connected channels (worker {worker_id})")
        })?;

        debug!(worker_id, "worker channel connected");

        Ok(Box::new(MainThreadWorkerHandle {
            worker_id,
            task_tx: Some(task_tx),
            ready_rx: Some(ready_rx),
        }))
    }
}

/// Handle connected to a PHP worker process via channels.
pub struct MainThreadWorkerHandle {
    worker_id: u32,
    task_tx: Option<mpsc::Sender<bridge::TaskRequest>>,
    ready_rx: Option<oneshot::Receiver<()>>,
}

#[async_trait]
impl WorkerHandle for MainThreadWorkerHandle {
    fn id(&self) -> u32 {
        self.worker_id
    }

    async fn ready(&mut self) -> Result<()> {
        if let Some(rx) = self.ready_rx.take() {
            rx.await
                .map_err(|_| anyhow::anyhow!("worker died before ready"))?;
        }
        Ok(())
    }

    async fn execute(&mut self, method: &str, payload: Bytes) -> Result<Bytes> {
        let tx = self
            .task_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("worker terminated"))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(bridge::TaskRequest {
            method: method.to_string(),
            payload,
            reply: reply_tx,
        })
        .await
        .map_err(|_| anyhow::anyhow!("worker process gone"))?;

        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("worker dropped reply"))?
    }

    async fn terminate(&mut self) -> Result<()> {
        self.task_tx.take();
        Ok(())
    }
}
