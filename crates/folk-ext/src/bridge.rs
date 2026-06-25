//! PHP bridge: thread-local worker state for channel communication.
//!
//! Uses `std::sync` channels (not tokio) so worker threads don't need
//! a tokio runtime. This also works correctly across `fork()`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;

use bytes::Bytes;
use folk_api::{ResponseChunk, WorkerError};
use tracing::debug;

/// A request sent from the server to a worker thread.
pub struct TaskRequest {
    /// Globally-unique per-request id (UUID v7). Empty `""` means "no request".
    pub request_id: Arc<str>,
    pub method: String,
    pub payload: serde_json::Value,
    /// Streaming reply channel. All response chunks (including the final
    /// `ResponseChunk::End`) must be sent here before `done_tx` is signalled.
    pub stream_tx: tokio::sync::mpsc::Sender<ResponseChunk>,
    /// Signals to the caller that PHP has finished handling the request (all
    /// chunks sent). The supervisor awaits this before accepting the next job.
    pub done_tx: tokio::sync::oneshot::Sender<()>,
    /// Streaming request body. `Some` only when the HTTP plugin dispatched the
    /// request before reading the body (`stream_request_body`). PHP pulls chunks
    /// via `folk_read()` / `folk_read_all()`; the channel closing means EOF.
    /// `None` in the buffered path — the body is already in `payload`.
    pub body_rx: Option<tokio::sync::mpsc::Receiver<Bytes>>,
}

/// Thread-local state for the current worker.
struct WorkerState {
    worker_id: u32,
    task_rx: mpsc::Receiver<TaskRequest>,
    ready_tx: Option<mpsc::SyncSender<()>>,
    current_stream_tx: Option<tokio::sync::mpsc::Sender<ResponseChunk>>,
    current_done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// True once PHP has called `folk_write_head` for the current request.
    stream_started: bool,
    /// Id of the request currently being handled on this thread (`None` if none).
    /// Exposed to PHP via `folk_request_id()`.
    current_request_id: Option<Arc<str>>,
    /// Streaming request body for the in-flight request. `folk_read` pulls from
    /// here; channel close = EOF. `None` outside a request or in buffered mode.
    current_body_rx: Option<tokio::sync::mpsc::Receiver<Bytes>>,
    /// Unconsumed tail of the last body chunk read from `current_body_rx`
    /// (a single `folk_read(len)` may consume less than a full chunk).
    current_body_leftover: Bytes,
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
            current_stream_tx: None,
            current_done_tx: None,
            stream_started: false,
            current_request_id: None,
            current_body_rx: None,
            current_body_leftover: Bytes::new(),
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

/// Id of the request currently being handled on this thread.
///
/// Returns `None` when no request is in flight (or this isn't a worker thread).
/// Exposed to PHP via the `folk_request_id()` native function.
pub fn current_request_id() -> Option<Arc<str>> {
    WORKER.with(|w| {
        w.borrow()
            .as_ref()
            .and_then(|state| state.current_request_id.clone())
    })
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
            state.current_request_id = Some(req.request_id);
            state.current_stream_tx = Some(req.stream_tx);
            state.current_done_tx = Some(req.done_tx);
            state.current_body_rx = req.body_rx;
            state.current_body_leftover = Bytes::new();
            state.stream_started = false;
            Ok(Some((method, payload_bytes)))
        } else {
            debug!(worker_id = state.worker_id, "recv: channel closed");
            Ok(None)
        }
    })
}

/// Send a response (raw JSON bytes from PHP) via the JSON dispatch path.
///
/// Emits the worker's return value verbatim as [`ResponseChunk::Return`], unless
/// the value carries an `__error` key — then it becomes [`ResponseChunk::Error`]
/// (a fatal). Callers that use the streaming API call `folk_write_head` /
/// `folk_write` / `folk_write_end` directly instead.
pub fn do_send(data: &[u8]) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        let stream_tx = state.current_stream_tx.take().ok_or("no pending request")?;
        let done_tx = state
            .current_done_tx
            .take()
            .ok_or("no pending done channel")?;
        state.current_request_id = None;
        state.stream_started = false;
        state.current_body_rx = None;
        state.current_body_leftover = Bytes::new();

        let value: serde_json::Value = match serde_json::from_slice(data) {
            Ok(v) => v,
            Err(e) => {
                // Malformed JSON is a fatal: surface it as an Error chunk so the
                // consumer returns 500/502, not a silent 200 (regression #56).
                tracing::error!("PHP returned malformed JSON: {e}");
                let _ = stream_tx.blocking_send(ResponseChunk::Error(WorkerError::new(format!(
                    "PHP returned malformed JSON: {e}"
                ))));
                let _ = stream_tx.blocking_send(ResponseChunk::End);
                let _ = done_tx.send(());
                return Err("PHP returned malformed JSON");
            },
        };

        if value.get("__error").is_some() {
            let _ = stream_tx.blocking_send(ResponseChunk::Error(worker_error_from_value(&value)));
        } else {
            let _ = stream_tx.blocking_send(ResponseChunk::Return(value));
        }
        let _ = stream_tx.blocking_send(ResponseChunk::End);
        let _ = done_tx.send(());
        Ok(())
    })
}

/// Send a fatal error response.
pub fn do_send_error(message: &str) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        let stream_tx = state.current_stream_tx.take();
        let done_tx = state.current_done_tx.take();
        state.current_request_id = None;
        state.stream_started = false;
        state.current_body_rx = None;
        state.current_body_leftover = Bytes::new();

        tracing::error!("PHP returned error: {message}");
        if let Some(tx) = stream_tx {
            let _ = tx.blocking_send(ResponseChunk::Error(WorkerError::new(message)));
            let _ = tx.blocking_send(ResponseChunk::End);
        }
        if let Some(done_tx) = done_tx {
            let _ = done_tx.send(());
        }
        Ok(())
    })
}

// ── Streaming PHP API ────────────────────────────────────────────────────────

/// Start a streaming response: send status + headers. Must be called once per request.
#[allow(clippy::implicit_hasher)]
pub fn do_write_head(status: u16, headers: HashMap<String, String>) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        let stream_tx = state
            .current_stream_tx
            .as_ref()
            .ok_or("no pending request")?;

        if state.stream_started {
            return Err("folk_write_head called more than once for this request");
        }
        state.stream_started = true;

        stream_tx
            .blocking_send(ResponseChunk::Headers { status, headers })
            .map_err(|_| "stream receiver closed (client gone)")?;

        Ok(())
    })
}

/// Send a body chunk. `folk_write_head` must have been called first.
pub fn do_write(data: Bytes) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let state = w.borrow();
        let state = state.as_ref().ok_or("not in a worker thread")?;

        let stream_tx = state
            .current_stream_tx
            .as_ref()
            .ok_or("no pending request")?;

        if data.is_empty() {
            return Ok(());
        }

        stream_tx
            .blocking_send(ResponseChunk::Body(data))
            .map_err(|_| "stream receiver closed (client gone)")?;

        Ok(())
    })
}

/// Finish the streaming response. Signals that no more chunks will follow.
pub fn do_write_end() -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        let stream_tx = state.current_stream_tx.take().ok_or("no pending request")?;
        let done_tx = state
            .current_done_tx
            .take()
            .ok_or("no pending done channel")?;
        state.current_request_id = None;
        state.stream_started = false;
        state.current_body_rx = None;
        state.current_body_leftover = Bytes::new();

        let _ = stream_tx.blocking_send(ResponseChunk::End);
        drop(stream_tx);
        let _ = done_tx.send(());
        Ok(())
    })
}

// ── Streaming request body API ───────────────────────────────────────────────

/// Read up to `length` bytes of the request body, blocking until data is
/// available. Returns an empty `Vec` at end-of-body (or when the request was
/// not dispatched in streaming mode — `current_body_rx` is `None`).
///
/// Exposed to PHP via `folk_read()`. A single call may return fewer than
/// `length` bytes; the unconsumed tail of a received chunk is buffered for the
/// next call.
pub fn do_read(length: usize) -> Vec<u8> {
    WORKER.with(|w| {
        let mut state_ref = w.borrow_mut();
        let Some(state) = state_ref.as_mut() else {
            return Vec::new();
        };

        // Refill the leftover buffer from the channel if it's empty.
        if state.current_body_leftover.is_empty() {
            let Some(rx) = state.current_body_rx.as_mut() else {
                return Vec::new(); // no streaming body for this request
            };
            match rx.blocking_recv() {
                Some(chunk) => state.current_body_leftover = chunk,
                None => return Vec::new(), // EOF: channel closed
            }
        }

        let take = length.min(state.current_body_leftover.len());
        state.current_body_leftover.split_to(take).to_vec()
    })
}

/// Read the entire remaining request body, blocking until end-of-body.
///
/// Returns an empty `Vec` when there is no streaming body. Exposed to PHP via
/// `folk_read_all()`.
pub fn do_read_all() -> Vec<u8> {
    WORKER.with(|w| {
        let mut state_ref = w.borrow_mut();
        let Some(state) = state_ref.as_mut() else {
            return Vec::new();
        };

        let mut out = Vec::new();
        if !state.current_body_leftover.is_empty() {
            out.extend_from_slice(&state.current_body_leftover);
            state.current_body_leftover = Bytes::new();
        }
        if let Some(rx) = state.current_body_rx.as_mut() {
            while let Some(chunk) = rx.blocking_recv() {
                out.extend_from_slice(&chunk);
            }
        }
        out
    })
}

// ── run_dispatch_loop (zero-copy path) ──────────────────────────────────────

/// Run the dispatch loop directly from Rust, calling PHP via `call_user_function`.
///
/// This is the zero-copy path: `serde_json::Value` → zval → PHP handler → zval → Value.
/// No JSON encode/decode at all.
///
/// `dispatch_fn` is the name of a PHP function with signature:
/// `function(string $method, array $params): array`
pub fn run_dispatch_loop(dispatch_fn: &str) -> Result<(), &'static str> {
    WORKER.with(|w| {
        // Signal ready first.
        {
            let mut state = w.borrow_mut();
            let state = state.as_mut().ok_or("not in a worker thread")?;
            if let Some(tx) = state.ready_tx.take() {
                let _ = tx.send(());
                debug!(
                    worker_id = state.worker_id,
                    "worker signaled ready (dispatch loop)"
                );
            }
        }

        // Restore VCWD to project root after php_execute_script set it
        // to dirname(script). Without this, Composer proxy scripts like
        // vendor/bin/folk-server leave VCWD in vendor/bin/.
        if let Some(root) = crate::project_root() {
            let _ = crate::zts::chdir(&root.to_string_lossy());
        }

        // Main dispatch loop.
        loop {
            // Receive and destructure the request, then store channels in
            // thread-local state BEFORE calling PHP (the borrow must be released).
            let (method, payload) = {
                let mut state = w.borrow_mut();
                let state = state.as_mut().ok_or("not in a worker thread")?;
                if let Ok(req) = state.task_rx.recv() {
                    let TaskRequest {
                        request_id,
                        method,
                        payload,
                        stream_tx,
                        done_tx,
                        body_rx,
                    } = req;
                    // Expose the id to PHP (folk_request_id()) for the duration
                    // of this call. call_dispatch runs OUTSIDE this borrow, so a
                    // reentrant folk_request_id() from PHP can borrow WORKER safely.
                    state.current_request_id = Some(request_id);
                    state.current_stream_tx = Some(stream_tx);
                    state.current_done_tx = Some(done_tx);
                    state.current_body_rx = body_rx;
                    state.current_body_leftover = Bytes::new();
                    state.stream_started = false;
                    (method, payload)
                } else {
                    debug!(worker_id = state.worker_id, "dispatch loop: channel closed");
                    // Only the main thread (worker #1) should join ZTS workers.
                    // ZTS workers must NOT join — they'd deadlock trying to join themselves.
                    if state.worker_id == 1 {
                        crate::join_zts_workers();
                    }
                    return Ok(());
                }
            };

            // Call PHP handler directly: Value → zval → PHP → zval → Value.
            let result = crate::zts::call_dispatch(dispatch_fn, &method, &payload);

            // Finalise the stream based on what PHP did:
            // - If PHP called folk_write_head/write/end, the stream is already
            //   flushed; we just need to ensure End + done are sent if not already.
            // - If PHP returned a full response Value (backward compat), convert
            //   it to chunks now.
            WORKER.with(|ww| {
                let mut st = ww.borrow_mut();
                if let Some(st) = st.as_mut() {
                    let stream_started = st.stream_started;
                    let stream_tx = st.current_stream_tx.take();
                    let done_tx = st.current_done_tx.take();
                    st.current_request_id = None;
                    st.stream_started = false;
                    st.current_body_rx = None;
                    st.current_body_leftover = Bytes::new();

                    if let Some(tx) = stream_tx {
                        if stream_started {
                            // PHP already streamed via folk_write_*. Ensure End.
                            let _ = tx.blocking_send(ResponseChunk::End);
                        } else {
                            // Non-streaming handler: emit the return value
                            // verbatim (Return), or a fatal (Error).
                            match result {
                                Ok(value) => {
                                    if value.get("__error").is_some() {
                                        let _ = tx.blocking_send(ResponseChunk::Error(
                                            worker_error_from_value(&value),
                                        ));
                                    } else {
                                        let _ = tx.blocking_send(ResponseChunk::Return(value));
                                    }
                                },
                                Err(e) => {
                                    tracing::error!("PHP handler error: {e}");
                                    let _ = tx.blocking_send(ResponseChunk::Error(
                                        WorkerError::new(e.to_string()),
                                    ));
                                },
                            }
                            let _ = tx.blocking_send(ResponseChunk::End);
                        }
                        drop(tx);
                    }

                    if let Some(dtx) = done_tx {
                        let _ = dtx.send(());
                    }
                }
            });
        }
    })
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Build a [`WorkerError`] from a PHP `{__error, __error_class, __error_trace}`
/// value. The class and trace are present only when the SDK populated them
/// (dev mode); in production they are absent and stay `None`.
fn worker_error_from_value(value: &serde_json::Value) -> WorkerError {
    let message = value
        .get("__error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("worker error")
        .to_string();
    let exception_class = value
        .get("__error_class")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let stacktrace = value
        .get("__error_trace")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    WorkerError {
        message,
        exception_class,
        stacktrace,
    }
}
