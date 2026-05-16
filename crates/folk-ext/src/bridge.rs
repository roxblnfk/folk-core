//! PHP bridge: thread-local worker state for channel communication.
//!
//! Uses `std::sync` channels (not tokio) so worker threads don't need
//! a tokio runtime. This also works correctly across `fork()`.

use std::cell::RefCell;
use std::sync::mpsc;

use tracing::debug;

/// A request sent from the server to a worker thread.
pub struct TaskRequest {
    pub method: String,
    pub payload: serde_json::Value,
    pub reply: mpsc::SyncSender<anyhow::Result<serde_json::Value>>,
}

/// Thread-local state for the current worker.
struct WorkerState {
    worker_id: u32,
    task_rx: mpsc::Receiver<TaskRequest>,
    ready_tx: Option<mpsc::SyncSender<()>>,
    current_reply: Option<mpsc::SyncSender<anyhow::Result<serde_json::Value>>>,
}

thread_local! {
    static WORKER: RefCell<Option<WorkerState>> = const { RefCell::new(None) };
}

/// Initialize thread-local worker state.
pub fn init_worker_state(
    worker_id: u32,
    task_rx: mpsc::Receiver<TaskRequest>,
    ready_tx: mpsc::SyncSender<()>,
) {
    WORKER.with(|w| {
        *w.borrow_mut() = Some(WorkerState {
            worker_id,
            task_rx,
            ready_tx: Some(ready_tx),
            current_reply: None,
        });
    });
}

/// Clean up thread-local state.
pub fn cleanup_worker_state() {
    WORKER.with(|w| {
        *w.borrow_mut() = None;
    });
}

/// Returns true if this thread has worker bridge state initialized.
pub fn has_worker_state() -> bool {
    WORKER.with(|w| w.borrow().is_some())
}

/// Signal ready. Returns Ok(true) if sent, Ok(false) if already called.
pub fn do_ready() -> Result<bool, &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        if let Some(tx) = state.ready_tx.take() {
            let _ = tx.send(());
            debug!(worker_id = state.worker_id, "worker signaled ready");
            Ok(true)
        } else {
            Ok(false)
        }
    })
}

/// Block until a request arrives. Returns `(method, payload_json_bytes)` or `None` on shutdown.
///
/// Internally receives `serde_json::Value` from the channel and serializes to JSON bytes
/// for PHP consumption. PHP calls `json_decode()` on these bytes.
pub fn do_recv() -> Result<Option<(String, Vec<u8>)>, &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        if let Ok(req) = state.task_rx.recv() {
            let method = req.method.clone();
            // Value → JSON bytes for PHP (only serialization on the hot path)
            let payload_bytes = serde_json::to_vec(&req.payload).unwrap_or_default();
            state.current_reply = Some(req.reply);
            Ok(Some((method, payload_bytes)))
        } else {
            debug!(worker_id = state.worker_id, "recv: channel closed");
            Ok(None)
        }
    })
}

/// Send a successful response (raw JSON bytes from PHP).
///
/// Internally deserializes JSON bytes from PHP into `serde_json::Value`
/// for zero-copy return through the channel.
pub fn do_send(data: &[u8]) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        let reply = state.current_reply.take().ok_or("no pending request")?;
        // JSON bytes → Value (only deserialization on the hot path)
        let value: serde_json::Value =
            serde_json::from_slice(data).unwrap_or(serde_json::Value::Null);
        let _ = reply.send(Ok(value));
        Ok(())
    })
}

/// Send an error response.
pub fn do_send_error(message: &str) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        let reply = state.current_reply.take().ok_or("no pending request")?;
        let _ = reply.send(Err(anyhow::anyhow!("{message}")));
        Ok(())
    })
}
