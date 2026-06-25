//! PHP bridge: thread-local worker state for channel communication.
//!
//! Uses `std::sync` channels (not tokio) so worker threads don't need
//! a tokio runtime. This also works correctly across `fork()`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;

use bytes::Bytes;
use folk_api::ResponseChunk;
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
            state.stream_started = false;
            Ok(Some((method, payload_bytes)))
        } else {
            debug!(worker_id = state.worker_id, "recv: channel closed");
            Ok(None)
        }
    })
}

/// Send a successful response (raw JSON bytes from PHP).
///
/// Converts the JSON bytes from PHP into `ResponseChunk`s (Headers + Body + End)
/// for backward compatibility — callers that use the new streaming API can skip
/// this and call `folk_write_head` / `folk_write` / `folk_write_end` directly.
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

        let value: serde_json::Value = serde_json::from_slice(data).map_err(|e| {
            tracing::error!("PHP returned malformed JSON: {e}");
            "PHP returned malformed JSON"
        })?;

        send_value_as_chunks(&value, &stream_tx);
        let _ = done_tx.send(());
        Ok(())
    })
}

/// Send an error response.
pub fn do_send_error(message: &str) -> Result<(), &'static str> {
    WORKER.with(|w| {
        let mut state = w.borrow_mut();
        let state = state.as_mut().ok_or("not in a worker thread")?;

        // Close the stream (dropping stream_tx signals channel closed to consumer).
        state.current_stream_tx.take();
        state.current_request_id = None;
        state.stream_started = false;

        if let Some(done_tx) = state.current_done_tx.take() {
            tracing::error!("PHP returned error: {message}");
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

        let _ = stream_tx.blocking_send(ResponseChunk::End);
        drop(stream_tx);
        let _ = done_tx.send(());
        Ok(())
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
                    } = req;
                    // Expose the id to PHP (folk_request_id()) for the duration
                    // of this call. call_dispatch runs OUTSIDE this borrow, so a
                    // reentrant folk_request_id() from PHP can borrow WORKER safely.
                    state.current_request_id = Some(request_id);
                    state.current_stream_tx = Some(stream_tx);
                    state.current_done_tx = Some(done_tx);
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

                    if let Some(tx) = stream_tx {
                        if stream_started {
                            // PHP already sent headers. Ensure End is sent.
                            let _ = tx.blocking_send(ResponseChunk::End);
                        } else {
                            // Backward compat: convert return Value to chunks.
                            match result {
                                Ok(ref value) => send_value_as_chunks(value, &tx),
                                Err(ref e) => {
                                    tracing::error!("PHP handler error: {e}");
                                    // Close the stream without sending chunks.
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

/// Convert a legacy `{status, headers, body}` Value into `ResponseChunks` and
/// send them synchronously via `blocking_send` (safe from ZTS threads).
fn send_value_as_chunks(value: &serde_json::Value, tx: &tokio::sync::mpsc::Sender<ResponseChunk>) {
    let status = u16::try_from(
        value
            .get("status")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(200),
    )
    .unwrap_or(200);

    let headers: HashMap<String, String> = value
        .get("headers")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let _ = tx.blocking_send(ResponseChunk::Headers { status, headers });

    let body_str = value.get("body").and_then(|v| v.as_str()).unwrap_or("");
    let body_encoding = value.get("body_encoding").and_then(|v| v.as_str());

    let body_bytes: Bytes = if body_encoding == Some("base64") {
        use base64::Engine;
        match base64::engine::general_purpose::STANDARD.decode(body_str) {
            Ok(b) => Bytes::from(b),
            Err(e) => {
                tracing::error!("send_value_as_chunks: base64 decode failed: {e}");
                Bytes::new()
            },
        }
    } else {
        Bytes::from(body_str.as_bytes().to_vec())
    };

    if !body_bytes.is_empty() {
        let _ = tx.blocking_send(ResponseChunk::Body(body_bytes));
    }
}
