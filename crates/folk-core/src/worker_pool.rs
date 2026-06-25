//! Worker pool: dispatches requests to PHP workers, manages slot lifecycle.
//!
//! See `folk-spec/spec/03-worker-lifecycle.md` for the design.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use folk_api::{Executor, ResponseChunk};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::config::WorkersConfig;
use crate::runtime::{Runtime, WorkerHandle};
use crate::worker_slot::SlotInfo;

/// Pool errors. Plugins typically translate these into their own domain errors.
#[derive(Debug, thiserror::Error)]
pub enum WorkError {
    #[error("all workers busy")]
    Busy,
    #[error("worker died during request")]
    WorkerDied,
    #[error("execution timed out")]
    Timeout,
    #[error("worker returned application error: {message}")]
    Application { code: i32, message: String },
    #[error("internal error: {0}")]
    Internal(String),
}

/// One dispatch request: id, method name, payload + streaming reply channel.
///
/// The `permit` is released when the PHP worker finishes the request (sends
/// `ResponseChunk::End`). It is moved into the bridge thread-local and dropped
/// there — `OwnedSemaphorePermit::drop` is atomic and safe from any thread.
struct DispatchRequest {
    request_id: Arc<str>,
    method: String,
    payload: serde_json::Value,
    stream_tx: mpsc::Sender<ResponseChunk>,
    permit: OwnedSemaphorePermit,
}

/// Worker pool — the dispatch surface.
pub struct WorkerPool {
    request_tx: mpsc::Sender<DispatchRequest>,
    semaphore: Arc<Semaphore>,
    runtime: Arc<dyn Runtime>,
    /// Monotonic reload generation. Bumped by `trigger_reload`; observed by
    /// slot supervisors to recycle their workers after the current request.
    reload_tx: watch::Sender<u64>,
    _pool_task: JoinHandle<()>,
}

impl WorkerPool {
    /// Construct a pool with `config.count` workers spawned via `runtime`.
    ///
    /// Returns once the pool task is started. Workers boot asynchronously
    /// in the background.
    pub fn new(runtime: Arc<dyn Runtime>, config: WorkersConfig) -> Result<Arc<Self>> {
        let semaphore = Arc::new(Semaphore::new(config.count));
        let (request_tx, request_rx) = mpsc::channel::<DispatchRequest>(1024);
        let (reload_tx, reload_rx) = watch::channel(0u64);

        let pool_task = tokio::spawn(pool_main(
            runtime.clone(),
            config,
            request_rx,
            semaphore.clone(),
            reload_rx,
        ));

        Ok(Arc::new(Self {
            request_tx,
            semaphore,
            runtime,
            reload_tx,
            _pool_task: pool_task,
        }))
    }

    /// Trigger a hot reload: invalidate compiled-code caches, then signal all
    /// recyclable workers to restart after their current request completes.
    ///
    /// Non-recyclable workers (the main PHP thread) keep running — see the
    /// dev-mode docs for the implications.
    pub async fn trigger_reload(&self) {
        if let Err(e) = self.runtime.reload().await {
            warn!(error = %e, "reload: cache invalidation failed; recycling anyway");
        }
        self.reload_tx.send_modify(|g| *g += 1);
        let generation = *self.reload_tx.borrow();
        info!(generation, "hot reload triggered; recycling workers");
    }

    /// Dispatch a streaming request through the pool.
    ///
    /// Returns an mpsc receiver of [`ResponseChunk`]s together with the
    /// `request_id` (UUID v7) for this request.  The semaphore permit is
    /// transferred into the bridge thread-local and dropped when PHP sends
    /// `ResponseChunk::End`, freeing the worker slot for the next request.
    ///
    /// Callers must drain the receiver to `End` (or until the channel closes)
    /// to allow the worker to proceed.
    async fn dispatch_streamed(
        &self,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<(mpsc::Receiver<ResponseChunk>, Arc<str>)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .context("pool semaphore closed")?;

        // UUID v7: time-ordered and globally unique across instances/restarts.
        let request_id: Arc<str> = Arc::from(uuid::Uuid::now_v7().hyphenated().to_string());
        let (stream_tx, stream_rx) = mpsc::channel(64);

        self.request_tx
            .send(DispatchRequest {
                request_id: request_id.clone(),
                method: method.to_string(),
                payload,
                stream_tx,
                permit,
            })
            .await
            .map_err(|_| anyhow!("pool task gone"))?;

        Ok((stream_rx, request_id))
    }
}

#[async_trait]
impl Executor for WorkerPool {
    async fn execute_method(&self, method: &str, payload: Bytes) -> Result<Bytes> {
        debug!(method, payload_len = payload.len(), "pool: execute_method");
        let value: serde_json::Value =
            serde_json::from_slice(&payload).context("pool: failed to parse payload as JSON")?;
        let (mut rx, _id) = self.dispatch_streamed(method, value).await?;
        // Collect the full response by draining the stream.
        let result = collect_stream(&mut rx).await?;
        let bytes = serde_json::to_vec(&result).context("pool: failed to serialize response")?;
        Ok(Bytes::from(bytes))
    }

    async fn execute_value(
        &self,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value> {
        debug!(method, "pool: execute_value");
        let (mut rx, _id) = self.dispatch_streamed(method, payload).await?;
        collect_stream(&mut rx).await
    }

    async fn execute_value_traced(
        &self,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<(serde_json::Value, Arc<str>)> {
        debug!(method, "pool: execute_value_traced");
        let (mut rx, id) = self.dispatch_streamed(method, payload).await?;
        let value = collect_stream(&mut rx).await?;
        Ok((value, id))
    }

    async fn execute_streamed(
        &self,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<(mpsc::Receiver<ResponseChunk>, Arc<str>)> {
        debug!(method, "pool: execute_streamed");
        self.dispatch_streamed(method, payload).await
    }
}

/// Drain a `ResponseChunk` stream into a single `serde_json::Value`.
///
/// Collects `Headers` and `Body` chunks; ignores `End`. Used by the
/// non-streaming `execute_method` / `execute_value` paths for backward compat.
async fn collect_stream(rx: &mut mpsc::Receiver<ResponseChunk>) -> Result<serde_json::Value> {
    let mut status: u16 = 200;
    let mut headers = std::collections::HashMap::new();
    let mut body_bytes: Vec<u8> = Vec::new();

    while let Some(chunk) = rx.recv().await {
        match chunk {
            ResponseChunk::Headers {
                status: s,
                headers: h,
            } => {
                status = s;
                headers = h;
            },
            ResponseChunk::Body(b) => body_bytes.extend_from_slice(&b),
            ResponseChunk::End => break,
        }
    }

    // Reconstruct the legacy Value format that HTTP plugin / callers expect.
    let headers_value: serde_json::Value = headers
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect::<serde_json::Map<_, _>>()
        .into();

    Ok(serde_json::json!({
        "status": status,
        "headers": headers_value,
        "body": String::from_utf8_lossy(&body_bytes),
    }))
}

// ---- Pool task ---------------------------------------------------------------

async fn pool_main(
    runtime: Arc<dyn Runtime>,
    config: WorkersConfig,
    mut request_rx: mpsc::Receiver<DispatchRequest>,
    _semaphore: Arc<Semaphore>,
    reload_rx: watch::Receiver<u64>,
) {
    let mut slot_inboxes: Vec<mpsc::Sender<DispatchRequest>> = Vec::with_capacity(config.count);
    let mut slot_supervisors: Vec<JoinHandle<()>> = Vec::with_capacity(config.count);

    for slot_id in 0..config.count {
        let (slot_tx, slot_rx) = mpsc::channel::<DispatchRequest>(8);
        slot_inboxes.push(slot_tx);
        let runtime_clone = runtime.clone();
        let cfg_clone = config.clone();
        let reload_clone = reload_rx.clone();
        let supervisor = tokio::spawn(slot_supervisor(
            slot_id,
            runtime_clone,
            cfg_clone,
            slot_rx,
            reload_clone,
        ));
        slot_supervisors.push(supervisor);
    }

    // Round-robin dispatch: on SendError the slot's inbox is permanently closed
    // (its supervisor task exited/panicked). Mark the slot dead, recover the
    // request value from the error and try the next live slot.
    let n = slot_inboxes.len();
    let mut next: usize = 0;
    let mut dead: Vec<bool> = vec![false; n];

    while let Some(initial_req) = request_rx.recv().await {
        // Wrap in Option so we can move into send() and recover ownership on
        // failure — the Rust borrow checker cannot track that `req = e.0`
        // in the Err arm restores ownership through the loop.
        let mut req: Option<DispatchRequest> = Some(initial_req);
        let mut sent = false;

        for attempt in 0..n {
            let chosen = (next.wrapping_add(attempt)) % n;
            if dead[chosen] {
                continue;
            }
            let r = req.take().expect("req must be Some when slot is alive");
            match slot_inboxes[chosen].send(r).await {
                Ok(()) => {
                    next = chosen.wrapping_add(1);
                    sent = true;
                    break;
                },
                Err(e) => {
                    warn!(slot_id = chosen, "slot inbox closed; skipping dead slot");
                    dead[chosen] = true;
                    req = Some(e.0);
                },
            }
        }

        if !sent {
            error!("all worker slot inboxes closed; cannot dispatch request");
            // permit drops with req — semaphore slot released on error
        }
    }

    info!("pool main loop exiting; awaiting supervisors");
    for handle in slot_supervisors {
        let _ = handle.await;
    }
}

// ---- Slot supervisor ---------------------------------------------------------

async fn slot_supervisor(
    slot_id: usize,
    runtime: Arc<dyn Runtime>,
    config: WorkersConfig,
    mut inbox: mpsc::Receiver<DispatchRequest>,
    mut reload_rx: watch::Receiver<u64>,
) {
    let mut slot = SlotInfo::new();
    let mut worker: Option<Box<dyn WorkerHandle>> = None;
    // Reload generation this worker was booted at. When the pool's generation
    // advances past this, the worker must be recycled to pick up new code.
    let mut boot_generation: u64 = *reload_rx.borrow();

    loop {
        // Spawn a worker if we don't have one.
        if worker.is_none() {
            boot_generation = *reload_rx.borrow();
            match boot_worker(&runtime, &config, &mut slot).await {
                Ok(w) => worker = Some(w),
                Err(e) => {
                    error!(slot_id, error = ?e, "failed to boot worker, will retry");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                },
            }
        }

        let recyclable = worker.as_ref().is_some_and(|w| w.is_recyclable());

        // Wait for a request, a reload signal, or shutdown.
        let req = tokio::select! {
            biased;
            // React to a reload while idle so workers restart promptly even
            // without traffic. Skip for non-recyclable workers (main thread).
            res = reload_rx.changed(), if recyclable => {
                if res.is_err() {
                    // Pool dropped the sender — treat as shutdown.
                    info!(slot_id, "supervisor shutting down (reload channel closed)");
                    if let Some(mut w) = worker.take() {
                        let _ = w.terminate().await;
                    }
                    return;
                }
                if *reload_rx.borrow() > boot_generation {
                    info!(slot_id, "recycling idle worker for hot reload");
                    if let Some(mut w) = worker.take() {
                        let _ = w.terminate().await;
                    }
                    slot = SlotInfo::new();
                }
                continue;
            },
            maybe_req = inbox.recv() => {
                let Some(req) = maybe_req else {
                    info!(slot_id, "supervisor shutting down (inbox closed)");
                    if let Some(mut w) = worker.take() {
                        if let Err(e) = w.terminate().await {
                            warn!(slot_id, error = ?e, "terminate error during shutdown");
                        }
                    }
                    return;
                };
                req
            },
        };

        // Dispatch — destructure req so stream_tx and permit are clearly owned.
        let DispatchRequest {
            request_id,
            method,
            payload,
            stream_tx,
            permit,
        } = req;

        let Some(w) = worker.as_mut() else {
            unreachable!()
        };
        slot.mark_busy();
        let exec_result = tokio::time::timeout(
            config.exec_timeout,
            w.execute_streaming(&method, payload, request_id.clone(), stream_tx),
        )
        .await;
        // Release the semaphore slot now that PHP has finished (or timed out).
        drop(permit);
        slot.mark_idle();

        if let Err(e) = match exec_result {
            Ok(r) => r,
            Err(_) => Err(anyhow::anyhow!("worker execution timed out")),
        } {
            warn!(slot_id, error = ?e, "streaming dispatch error");
        }

        // Recycle on reload (after the request completes) or per the lifecycle
        // policies (max_jobs / ttl).
        let reload_pending = *reload_rx.borrow() > boot_generation;
        if reload_pending || slot.should_recycle(&config) {
            if let Some(ref w) = worker {
                if !w.is_recyclable() {
                    debug!(slot_id, "skipping recycle for non-recyclable worker");
                    continue;
                }
            }
            let reason = if reload_pending {
                "hot reload"
            } else {
                "lifecycle"
            };
            info!(
                slot_id,
                jobs = slot.jobs_handled,
                reason,
                "recycling worker"
            );
            if let Some(mut w) = worker.take() {
                let _ = w.terminate().await;
            }
            slot = SlotInfo::new();
        }
    }
}

async fn boot_worker(
    runtime: &Arc<dyn Runtime>,
    config: &WorkersConfig,
    slot: &mut SlotInfo,
) -> Result<Box<dyn WorkerHandle>> {
    debug!("boot_worker: spawning");
    let mut handle = runtime.spawn().await.context("spawn")?;
    debug!(id = handle.id(), "boot_worker: waiting for ready");

    let timeout = tokio::time::timeout(config.boot_timeout, handle.ready());
    match timeout.await {
        Ok(Ok(())) => {
            let id = handle.id();
            slot.mark_ready(id);
            debug!(id, "worker ready");
            Ok(handle)
        },
        Ok(Err(e)) => {
            let _ = handle.terminate().await;
            Err(e).context("worker ready() failed during boot")
        },
        Err(_) => {
            let _ = handle.terminate().await;
            anyhow::bail!("worker boot timed out after {:?}", config.boot_timeout)
        },
    }
}
