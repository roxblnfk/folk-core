//! Extension runtime: the main PHP thread acts as a worker.
//!
//! On NTS PHP, only the main thread can execute PHP. The tokio runtime
//! runs in a background thread; requests are sent to the main thread
//! via channels. The main thread calls folk_worker_recv/send.
//!
//! The runtime is "pre-connected": spawn() returns a handle that's
//! already wired to channels set up before the server starts.

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

/// Runtime that connects workers to the main PHP thread via channels.
pub struct ExtensionRuntime {
    #[allow(dead_code)]
    config: WorkersConfig,
    /// Pre-created task sender for the main thread worker.
    /// Taken on first spawn() call.
    main_task_tx: std::sync::Mutex<Option<mpsc::Sender<bridge::TaskRequest>>>,
    /// Pre-created ready receiver for the main thread worker.
    main_ready_rx: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

impl ExtensionRuntime {
    /// Create a runtime with pre-connected channels for the main thread.
    pub fn new(
        config: WorkersConfig,
        task_tx: mpsc::Sender<bridge::TaskRequest>,
        ready_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            config,
            main_task_tx: std::sync::Mutex::new(Some(task_tx)),
            main_ready_rx: std::sync::Mutex::new(Some(ready_rx)),
        }
    }
}

#[async_trait]
impl Runtime for ExtensionRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);

        // First worker: use the pre-connected main thread channels.
        let task_tx = self.main_task_tx.lock().unwrap().take().ok_or_else(|| {
            anyhow::anyhow!("only 1 worker supported in NTS mode (worker {worker_id} requested)")
        })?;

        let ready_rx = self
            .main_ready_rx
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| anyhow::anyhow!("ready channel already taken"))?;

        debug!(worker_id, "main thread worker connected");

        Ok(Box::new(MainThreadWorkerHandle {
            worker_id,
            task_tx: Some(task_tx),
            ready_rx: Some(ready_rx),
        }))
    }
}

/// Handle to the main PHP thread worker.
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
                .map_err(|_| anyhow::anyhow!("main thread died before ready"))?;
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
        .map_err(|_| anyhow::anyhow!("main thread gone"))?;

        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("main thread dropped reply"))?
    }

    async fn terminate(&mut self) -> Result<()> {
        self.task_tx.take();
        Ok(())
    }
}
