//! PHP bridge: thread-local worker state for channel communication.
//!
//! Uses `std::sync` channels (not tokio) so worker threads don't need
//! a tokio runtime. This also works correctly across `fork()`.

use std::cell::RefCell;
use std::sync::mpsc;

use bytes::Bytes;
use tracing::debug;

/// A request sent from the server to a worker thread.
pub struct TaskRequest {
    pub method: String,
    pub payload: Bytes,
    pub reply: mpsc::SyncSender<anyhow::Result<Bytes>>,
}

/// Thread-local state for the current worker.
struct WorkerState {
    worker_id: u32,
    task_rx: mpsc::Receiver<TaskRequest>,
    ready_tx: Option<mpsc::SyncSender<()>>,
    current_reply: Option<mpsc::SyncSender<anyhow::Result<Bytes>>>,
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

/// Block until a request arrives. Returns `(method, payload_bytes)` or `None` on shutdown.
pub fn do_recv() -> Result<Option<(String, Vec<u8>)>, &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        if let Ok(req) = state.task_rx.recv() {
            let method = req.method.clone();
            let payload = req.payload.to_vec();
            state.current_reply = Some(req.reply);
            Ok(Some((method, payload)))
        } else {
            debug!(worker_id = state.worker_id, "recv: channel closed");
            Ok(None)
        }
    })
}

/// Send a successful response (raw bytes).
pub fn do_send(data: Vec<u8>) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        let reply = state.current_reply.take().ok_or("no pending request")?;
        let _ = reply.send(Ok(Bytes::from(data)));
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
