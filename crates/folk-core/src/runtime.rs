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
    async fn execute(
        &mut self,
        method: &str,
        payload: serde_json::Value,
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
}

// --- MockRuntime: in-memory runtime for tests ---

type MockResponder =
    std::sync::Arc<dyn Fn(&str, &serde_json::Value) -> Result<serde_json::Value> + Send + Sync>;

/// In-memory runtime used in tests. Each spawned worker echoes requests back.
pub struct MockRuntime {
    responder: MockResponder,
    next_id: std::sync::atomic::AtomicU32,
}

impl MockRuntime {
    /// Create a mock runtime that echoes the payload back as the result.
    pub fn echo() -> Self {
        Self {
            responder: std::sync::Arc::new(|_method, payload| Ok(payload.clone())),
            next_id: std::sync::atomic::AtomicU32::new(10000),
        }
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
            terminated: false,
        }))
    }
}

/// In-memory worker used by `MockRuntime`.
pub struct MockWorker {
    id: u32,
    responder: MockResponder,
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
    ) -> Result<serde_json::Value> {
        if self.terminated {
            anyhow::bail!("worker terminated");
        }
        (self.responder)(method, &payload)
    }

    async fn terminate(&mut self) -> Result<()> {
        self.terminated = true;
        Ok(())
    }
}
