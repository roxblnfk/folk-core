//! Abstraction over how PHP workers are spawned.
//!
//! `WorkerPool` does not know what a PHP process is. It asks the
//! `Runtime` to spawn a worker; the runtime returns a `WorkerHandle` which
//! gives the pool a way to execute requests and to terminate the worker.
//!
//! In phase 23 (extension mode), the runtime spawns OS threads that run PHP
//! inside the same process. Communication is via channels (zero IPC).

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use folk_api::ResponseChunk;
use tokio::sync::mpsc;

/// A handle to a spawned worker.
///
/// The pool dispatches requests via `execute` and controls lifecycle via
/// `ready` and `terminate`.
#[async_trait]
pub trait WorkerHandle: Send + 'static {
    /// Worker identifier (thread ID, PID, or synthetic).
    fn id(&self) -> u32;

    /// Wait for the worker to signal readiness.
    /// Returns once the worker has booted and is ready to accept requests.
    async fn ready(&mut self) -> Result<()>;

    /// Execute a single request: send structured data, receive result.
    ///
    /// `request_id` is a globally-unique id (UUID v7) for this request, exposed
    /// to PHP via `folk_request_id()` for log correlation.
    async fn execute(
        &mut self,
        method: &str,
        payload: serde_json::Value,
        request_id: Arc<str>,
    ) -> Result<serde_json::Value>;

    /// Terminate the worker. Implementations should signal shutdown and
    /// wait for the worker to exit.
    async fn terminate(&mut self) -> Result<()>;

    /// Whether this worker can be recycled (terminated and respawned).
    /// Returns `false` for the main thread worker which cannot be restarted.
    fn is_recyclable(&self) -> bool {
        true
    }

    /// Execute a request with a streaming reply channel.
    ///
    /// Chunks (including the final [`ResponseChunk::End`]) are sent to
    /// `stream_tx`. Returns `Ok(())` when the worker has finished the request
    /// and all chunks have been sent.
    ///
    /// `body_rx`, when `Some`, streams the request body to PHP (`folk_read`);
    /// `None` means the body is already embedded in `payload` (buffered mode).
    ///
    /// The default implementation returns an error — concrete runtimes that
    /// support streaming must override this method.
    async fn execute_streaming(
        &mut self,
        _method: &str,
        _payload: serde_json::Value,
        _request_id: Arc<str>,
        _stream_tx: mpsc::Sender<ResponseChunk>,
        _body_rx: Option<mpsc::Receiver<Bytes>>,
    ) -> Result<()> {
        anyhow::bail!("execute_streaming not supported by this runtime")
    }
}

/// Spawns workers per a runtime-specific strategy.
#[async_trait]
pub trait Runtime: Send + Sync + 'static {
    /// Spawn a single worker and return a handle.
    ///
    /// The caller must call `ready()` before dispatching requests.
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>>;

    /// Warm up shared caches (OPcache) before spawning workers.
    /// Called once at startup. Default: no-op.
    async fn warmup(&self) -> Result<()> {
        Ok(())
    }

    /// Invalidate compiled-code caches so respawned workers pick up changes.
    ///
    /// Called by the dev-mode file watcher just before workers are recycled.
    /// In ZTS the OPcache is shared process-wide, so a single reset affects
    /// every worker thread. Default: no-op.
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}

// --- MockRuntime: in-memory runtime for tests ---

type MockResponder =
    std::sync::Arc<dyn Fn(&str, &serde_json::Value) -> Result<serde_json::Value> + Send + Sync>;

/// In-memory runtime used in tests. Each spawned worker echoes requests back.
pub struct MockRuntime {
    responder: MockResponder,
    next_id: std::sync::atomic::AtomicU32,
    /// Request ids observed by all workers, in dispatch order. For test assertions.
    seen_request_ids: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    /// Spawn indices (0-based) whose workers should panic on the first execute call.
    panic_slots: std::collections::HashSet<u32>,
}

impl MockRuntime {
    /// Create a mock runtime that echoes the payload back as the result.
    pub fn echo() -> Self {
        Self {
            responder: std::sync::Arc::new(|_method, payload| Ok(payload.clone())),
            next_id: std::sync::atomic::AtomicU32::new(10000),
            seen_request_ids: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            panic_slots: std::collections::HashSet::default(),
        }
    }

    /// Create a runtime where workers at the given spawn indices (0-based) panic
    /// inside `execute`. The panic propagates through the supervisor task, dropping
    /// the slot's inbox receiver — used to test dead-slot detection in the pool.
    pub fn with_panicking_slots(indices: &[u32]) -> Self {
        Self {
            responder: std::sync::Arc::new(|_method, payload| Ok(payload.clone())),
            next_id: std::sync::atomic::AtomicU32::new(10000),
            seen_request_ids: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            panic_slots: indices.iter().copied().collect(),
        }
    }

    /// Number of workers spawned so far. Useful for asserting recycling.
    pub fn spawn_count(&self) -> u32 {
        self.next_id.load(std::sync::atomic::Ordering::Relaxed) - 10000
    }

    /// Request ids passed to `execute`, in the order they were dispatched.
    pub fn seen_request_ids(&self) -> Vec<String> {
        self.seen_request_ids.lock().unwrap().clone()
    }
}

#[async_trait]
impl Runtime for MockRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let spawn_index = id - 10000;
        let should_panic = self.panic_slots.contains(&spawn_index);
        Ok(Box::new(MockWorker {
            id,
            responder: self.responder.clone(),
            seen_request_ids: self.seen_request_ids.clone(),
            terminated: false,
            should_panic,
        }))
    }
}

/// In-memory worker used by `MockRuntime`.
pub struct MockWorker {
    id: u32,
    responder: MockResponder,
    seen_request_ids: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    terminated: bool,
    /// If true, `execute` panics — used to simulate a supervisor task crash.
    should_panic: bool,
}

#[async_trait]
impl WorkerHandle for MockWorker {
    fn id(&self) -> u32 {
        self.id
    }

    async fn ready(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(
        &mut self,
        method: &str,
        payload: serde_json::Value,
        request_id: Arc<str>,
    ) -> Result<serde_json::Value> {
        assert!(!self.should_panic, "simulated slot failure");
        if self.terminated {
            anyhow::bail!("worker terminated");
        }
        self.seen_request_ids
            .lock()
            .unwrap()
            .push(request_id.to_string());
        (self.responder)(method, &payload)
    }

    async fn execute_streaming(
        &mut self,
        method: &str,
        payload: serde_json::Value,
        request_id: Arc<str>,
        stream_tx: mpsc::Sender<ResponseChunk>,
        _body_rx: Option<mpsc::Receiver<Bytes>>,
    ) -> Result<()> {
        let value = self.execute(method, payload, request_id).await?;
        folk_api::value_to_chunks(value, &stream_tx).await;
        Ok(())
    }

    async fn terminate(&mut self) -> Result<()> {
        self.terminated = true;
        Ok(())
    }
}
