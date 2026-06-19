//! Extension runtime: workers via channels.
//!
//! Two modes:
//! - **Single-worker (NTS):** Main PHP thread is the worker. Pre-connected channels.
//! - **Multi-worker (ZTS):** Additional worker threads spawned from Rust,
//!   each with its own PHP context via TSRM.
//!
//! Uses `std::sync::mpsc` for task dispatch (worker thread blocks on recv).
//! Uses `tokio::sync::oneshot` for reply (no blocking on tokio side).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use folk_core::config::WorkersConfig;
use folk_core::runtime::{Runtime, WorkerHandle};
use tracing::{debug, info, warn};

use crate::bridge;
use crate::worker;
use crate::zts;

static NEXT_WORKER_ID: AtomicU32 = AtomicU32::new(1);

/// Tx side of a worker channel pair (kept by the runtime/pool).
pub struct WorkerTxSide {
    pub task_tx: mpsc::SyncSender<bridge::TaskRequest>,
    pub ready_rx: mpsc::Receiver<()>,
}

/// Extension runtime — manages worker channels and ZTS threads.
pub struct ExtensionRuntime {
    config: WorkersConfig,
    /// Pre-created channel pairs for NTS mode (main thread worker).
    channels: std::sync::Mutex<Vec<WorkerTxSide>>,
}

impl ExtensionRuntime {
    /// Create a runtime with pre-connected channels (for the main thread worker).
    pub fn new(config: WorkersConfig, tx_sides: Vec<WorkerTxSide>) -> Self {
        Self {
            config,
            channels: std::sync::Mutex::new(tx_sides),
        }
    }

    /// Spawn a ZTS worker thread with fresh channels.
    #[allow(clippy::unnecessary_wraps)] // Result for consistency with Runtime trait
    fn spawn_zts_worker(&self) -> Result<Box<dyn WorkerHandle>> {
        let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
        // ZTS threads may have a different CWD after php_request_startup(),
        // so resolve to absolute path before spawning.
        let script = std::env::current_dir()
            .unwrap_or_default()
            .join(&self.config.script)
            .to_string_lossy()
            .into_owned();

        let (task_tx, task_rx) = mpsc::sync_channel::<bridge::TaskRequest>(8);
        let (ready_tx, ready_rx) = mpsc::sync_channel::<()>(1);

        let thread_handle = worker::spawn_zts_worker(worker_id, script, task_rx, ready_tx);

        debug!(worker_id, "ZTS worker thread spawned");

        Ok(Box::new(ChannelWorkerHandle {
            worker_id,
            task_tx: Some(task_tx),
            ready_rx: Some(ready_rx),
            thread_handle: Some(thread_handle),
        }))
    }

    /// Take a pre-connected channel pair (for the main thread / NTS worker).
    fn take_preconnected(&self) -> Result<Box<dyn WorkerHandle>> {
        let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
        let tx_side = self.channels.lock().unwrap().pop().ok_or_else(|| {
            anyhow::anyhow!("no more pre-connected channels (worker {worker_id})")
        })?;

        debug!(worker_id, "pre-connected worker channel taken");

        Ok(Box::new(ChannelWorkerHandle {
            worker_id,
            task_tx: Some(tx_side.task_tx),
            ready_rx: Some(tx_side.ready_rx),
            thread_handle: None, // main thread — not managed by us
        }))
    }
}

#[async_trait]
impl Runtime for ExtensionRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        let has_preconnected = !self.channels.lock().unwrap().is_empty();

        if has_preconnected {
            self.take_preconnected()
        } else if self.config.count > 1 {
            self.spawn_zts_worker()
        } else {
            anyhow::bail!("no workers available and ZTS multi-worker not requested")
        }
    }

    async fn warmup(&self) -> Result<()> {
        if !zts::is_zts() {
            debug!("opcache warmup: skipping (NTS mode)");
            return Ok(());
        }

        let project_dir = std::env::current_dir().unwrap_or_default();
        let classmap_path = project_dir.join("vendor/composer/autoload_classmap.php");

        if !classmap_path.exists() {
            warn!("opcache warmup: vendor/composer/autoload_classmap.php not found, skipping");
            return Ok(());
        }

        let classmap_path_str = classmap_path.to_string_lossy().into_owned();
        let warmup_code = format!(
            r"
$classmap = require '{classmap_path_str}';
$loaded = 0;
foreach ($classmap as $class => $file) {{
    if (is_file($file) && function_exists('opcache_compile_file')) {{
        @opcache_compile_file($file);
        $loaded++;
    }}
}}
"
        );

        let start = Instant::now();

        // Run warmup in a dedicated thread (ZTS requires thread-local TSRM context).
        let result = tokio::task::spawn_blocking(move || {
            let _guard = zts::ZtsThreadGuard::new();
            zts::request_startup()?;
            let exec_result = zts::eval_string(&warmup_code);
            zts::request_shutdown();
            exec_result
        })
        .await
        .map_err(|e| anyhow::anyhow!("warmup thread panicked: {e}"))?;

        let elapsed = start.elapsed();
        let elapsed_ms = u32::try_from(elapsed.as_millis()).unwrap_or(u32::MAX);
        match result {
            Ok(()) => info!(elapsed_ms, "opcache warmup done"),
            Err(e) => {
                warn!(error = %e, elapsed_ms, "opcache warmup script failed");
            },
        }

        Ok(())
    }

    // NOTE: no `reload()` override. We deliberately do NOT run a dedicated ZTS
    // eval thread (e.g. `opcache_reset()`) on hot reload: a fresh TSRM thread
    // running concurrently with live worker threads being recycled races on
    // TSRM init/teardown and can SIGSEGV. Recycling re-bootstraps each worker,
    // which recompiles changed files when OPcache is off (the `php` CLI default)
    // or when `opcache.validate_timestamps=1` (the standard dev setting). For
    // hot reload in dev, keep `opcache.validate_timestamps=1` (do not set
    // `opcache.enable_cli=1` with `validate_timestamps=0`).
}

/// Handle connected to a worker via channels.
pub struct ChannelWorkerHandle {
    worker_id: u32,
    task_tx: Option<mpsc::SyncSender<bridge::TaskRequest>>,
    ready_rx: Option<mpsc::Receiver<()>>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

#[async_trait]
impl WorkerHandle for ChannelWorkerHandle {
    fn id(&self) -> u32 {
        self.worker_id
    }

    async fn ready(&mut self) -> Result<()> {
        if let Some(rx) = self.ready_rx.take() {
            tokio::task::spawn_blocking(move || rx.recv())
                .await
                .map_err(|e| anyhow::anyhow!("spawn_blocking panicked: {e}"))?
                .map_err(|_| anyhow::anyhow!("worker died before ready"))?;
        }
        Ok(())
    }

    async fn execute(
        &mut self,
        method: &str,
        payload: serde_json::Value,
        request_id: std::sync::Arc<str>,
    ) -> Result<serde_json::Value> {
        let tx = self
            .task_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("worker terminated"))?
            .clone();

        let method = method.to_string();

        // tokio oneshot for reply — send() is lock-free, recv() is async.
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        // SyncSender::send blocks only when channel is full (capacity=8).
        // With semaphore=4, at most 4 in-flight — never blocks.
        tx.send(bridge::TaskRequest {
            request_id,
            method,
            payload,
            reply: reply_tx,
        })
        .map_err(|_| anyhow::anyhow!("worker process gone"))?;

        // Await reply asynchronously — no spawn_blocking needed!
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("worker dropped reply"))?
    }

    async fn terminate(&mut self) -> Result<()> {
        // Close channel — dispatch loop will exit.
        self.task_tx.take();

        // Wait for ZTS thread to finish PHP cleanup (ts_free_thread etc).
        if let Some(handle) = self.thread_handle.take() {
            tokio::task::spawn_blocking(move || {
                let _ = handle.join();
            })
            .await
            .map_err(|e| anyhow::anyhow!("spawn_blocking panicked: {e}"))?;
        }

        Ok(())
    }

    fn is_recyclable(&self) -> bool {
        // Main thread (preconnected) cannot be recycled — it IS the PHP process.
        self.thread_handle.is_some()
    }
}
