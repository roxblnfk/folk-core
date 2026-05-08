//! Worker pool: dispatches requests to PHP workers, manages slot lifecycle.
//!
//! See `folk-spec/spec/03-worker-lifecycle.md` for the design.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use folk_api::Executor;
use folk_protocol::RpcMessage;
use rmpv::Value as RmpValue;
use tokio::sync::{Semaphore, mpsc, oneshot};
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
    #[error("protocol error: {0}")]
    Protocol(#[from] folk_protocol::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// One dispatch request: payload + reply channel.
struct DispatchRequest {
    payload: Bytes,
    reply: oneshot::Sender<Result<Bytes>>,
}

/// Worker pool — the dispatch surface.
pub struct WorkerPool {
    request_tx: mpsc::Sender<DispatchRequest>,
    semaphore: Arc<Semaphore>,
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

        let pool_task = tokio::spawn(pool_main(runtime, config, request_rx, semaphore.clone()));

        Ok(Arc::new(Self {
            request_tx,
            semaphore,
            _pool_task: pool_task,
        }))
    }
}

#[async_trait]
impl Executor for WorkerPool {
    async fn execute(&self, payload: Bytes) -> Result<Bytes> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .context("pool semaphore closed")?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(DispatchRequest {
                payload,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow!("pool task gone"))?;

        let result = reply_rx
            .await
            .map_err(|_| anyhow!("pool dropped reply channel"))?;

        drop(permit);
        result
    }
}

// ---- Pool task ---------------------------------------------------------------

async fn pool_main(
    runtime: Arc<dyn Runtime>,
    config: WorkersConfig,
    mut request_rx: mpsc::Receiver<DispatchRequest>,
    _semaphore: Arc<Semaphore>,
) {
    let mut slot_inboxes: Vec<mpsc::Sender<DispatchRequest>> = Vec::with_capacity(config.count);
    let mut slot_supervisors: Vec<JoinHandle<()>> = Vec::with_capacity(config.count);

    for slot_id in 0..config.count {
        let (slot_tx, slot_rx) = mpsc::channel::<DispatchRequest>(8);
        slot_inboxes.push(slot_tx);
        let runtime_clone = runtime.clone();
        let cfg_clone = config.clone();
        let supervisor = tokio::spawn(slot_supervisor(slot_id, runtime_clone, cfg_clone, slot_rx));
        slot_supervisors.push(supervisor);
    }

    // Round-robin dispatch.
    let mut next: usize = 0;
    while let Some(req) = request_rx.recv().await {
        let chosen = next % slot_inboxes.len();
        next = next.wrapping_add(1);

        if slot_inboxes[chosen].send(req).await.is_err() {
            warn!(slot_id = chosen, "slot inbox closed; failed to dispatch");
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
) {
    let mut slot = SlotInfo::new();
    let mut worker: Option<Box<dyn WorkerHandle>> = None;

    loop {
        // Spawn a worker if we don't have one.
        if worker.is_none() {
            match boot_worker(&runtime, &config, &mut slot).await {
                Ok(w) => worker = Some(w),
                Err(e) => {
                    error!(slot_id, error = ?e, "failed to boot worker, will retry");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                },
            }
        }

        let Some(w) = worker.as_mut() else {
            unreachable!()
        };

        // Wait for a request or for shutdown.
        let Some(req) = inbox.recv().await else {
            info!(slot_id, "supervisor shutting down (inbox closed)");
            if let Err(e) = w.terminate().await {
                warn!(slot_id, error = ?e, "terminate error during shutdown");
            }
            return;
        };

        // Dispatch.
        slot.mark_busy();
        let result = dispatch_one(w.as_mut(), req.payload, config.exec_timeout).await;
        slot.mark_idle();

        // Send reply.
        let _ = req.reply.send(result.map_err(anyhow::Error::from));

        // Recycle?
        if slot.should_recycle(&config) {
            info!(slot_id, jobs = slot.jobs_handled, "recycling worker");
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
    let mut handle = runtime.spawn().await.context("spawn")?;
    let timeout = tokio::time::timeout(config.boot_timeout, handle.recv_control());
    match timeout.await {
        Ok(Ok(Some(RpcMessage::Notify { method, .. }))) if method == "control.ready" => {
            let pid = handle.pid();
            slot.mark_ready(pid);
            debug!(pid, "worker ready");
            Ok(handle)
        },
        Ok(Ok(other)) => {
            let _ = handle.terminate().await;
            anyhow::bail!("expected control.ready, got {other:?}")
        },
        Ok(Err(e)) => {
            let _ = handle.terminate().await;
            Err(e).context("recv_control failed during boot")
        },
        Err(_) => {
            let _ = handle.terminate().await;
            anyhow::bail!("worker boot timed out after {:?}", config.boot_timeout)
        },
    }
}

async fn dispatch_one(
    worker: &mut dyn WorkerHandle,
    payload: Bytes,
    exec_timeout: Duration,
) -> Result<Bytes, WorkError> {
    static MSGID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let msgid = MSGID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let params = rmp_serde::from_slice::<RmpValue>(&payload)
        .map_err(|e| WorkError::Protocol(folk_protocol::Error::Decode(e)))?;
    let request = RpcMessage::request(msgid, "dispatch", params);

    worker
        .send_task(request)
        .await
        .map_err(|_| WorkError::WorkerDied)?;

    let recv = tokio::time::timeout(exec_timeout, worker.recv_task());
    let response = match recv.await {
        Ok(Ok(Some(msg))) => msg,
        Ok(Ok(None) | Err(_)) => return Err(WorkError::WorkerDied),
        Err(_) => return Err(WorkError::Timeout),
    };

    match response {
        RpcMessage::Response { error, result, .. } => {
            if !error.is_nil() {
                return Err(WorkError::Application {
                    code: -1,
                    message: format!("{error:?}"),
                });
            }
            let mut buf = Vec::new();
            rmp_serde::encode::write(&mut buf, &result)
                .map_err(|e| WorkError::Protocol(folk_protocol::Error::Encode(e)))?;
            Ok(Bytes::from(buf))
        },
        other => Err(WorkError::Protocol(folk_protocol::Error::InvalidFrame(
            format!("expected Response, got {other:?}"),
        ))),
    }
}
