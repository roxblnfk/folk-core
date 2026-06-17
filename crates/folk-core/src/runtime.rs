//! Abstraction over how PHP workers are spawned.
//!
//! `WorkerPool` does not know what a PHP process is. It asks the
//! `Runtime` to spawn a worker; the runtime returns a `WorkerHandle` which
//! gives the pool a way to execute requests and to terminate the worker.
//!
//! In phase 23 (extension mode), the runtime spawns OS threads that run PHP
//! inside the same process. Communication is via channels (zero IPC).

use anyhow::Result;
use async_trait::async_trait;

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
    /// `request_id` is a unique, monotonic id for this request, exposed to PHP
    /// via `folk_request_id()` for log correlation.
    async fn execute(
        &mut self,
        method: &str,
        payload: serde_json::Value,
        request_id: u64,
    ) -> Result<serde_json::Value>;

    /// Terminate the worker. Implementations should signal shutdown and
    /// wait for the worker to exit.
    async fn terminate(&mut self) -> Result<()>;

    /// Whether this worker can be recycled (terminated and respawned).
    /// Returns `false` for the main thread worker which cannot be restarted.
    fn is_recyclable(&self) -> bool {
        true
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
    seen_request_ids: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
}

impl MockRuntime {
    /// Create a mock runtime that echoes the payload back as the result.
    pub fn echo() -> Self {
        Self {
            responder: std::sync::Arc::new(|_method, payload| Ok(payload.clone())),
            next_id: std::sync::atomic::AtomicU32::new(10000),
            seen_request_ids: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Number of workers spawned so far. Useful for asserting recycling.
    pub fn spawn_count(&self) -> u32 {
        self.next_id.load(std::sync::atomic::Ordering::Relaxed) - 10000
    }

    /// Request ids passed to `execute`, in the order they were dispatched.
    pub fn seen_request_ids(&self) -> Vec<u64> {
        self.seen_request_ids.lock().unwrap().clone()
    }
}

#[async_trait]
impl Runtime for MockRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Box::new(MockWorker {
            id,
            responder: self.responder.clone(),
            seen_request_ids: self.seen_request_ids.clone(),
            terminated: false,
        }))
    }
}

/// In-memory worker used by `MockRuntime`.
pub struct MockWorker {
    id: u32,
    responder: MockResponder,
    seen_request_ids: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
    terminated: bool,
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
        request_id: u64,
    ) -> Result<serde_json::Value> {
        if self.terminated {
            anyhow::bail!("worker terminated");
        }
        self.seen_request_ids.lock().unwrap().push(request_id);
        (self.responder)(method, &payload)
    }

    async fn terminate(&mut self) -> Result<()> {
        self.terminated = true;
        Ok(())
    }
}
