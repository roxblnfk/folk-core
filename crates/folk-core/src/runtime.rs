//! Abstraction over how PHP workers are spawned.
//!
//! `WorkerPool` does not know what a PHP process is. It asks the
//! `Runtime` to spawn a worker; the runtime returns a `WorkerHandle` which
//! gives the pool ways to send/receive frames and to terminate the worker.
//!
//! `folk-runtime-pipe` (phase 4) implements this trait by spawning real PHP
//! processes via execve. `folk-runtime-fork` (phase 10) implements it via
//! prefork. Tests use `MockRuntime` below.

use std::collections::VecDeque;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use folk_protocol::RpcMessage;

/// A handle to a spawned worker.
///
/// The pool sends task-channel frames via `send_task` and receives them via
/// `recv_task`. Control-channel frames travel through `send_control` and
/// `recv_control`. Termination is via `terminate`.
#[async_trait]
pub trait WorkerHandle: Send + 'static {
    /// Worker process ID (informational; used for logging).
    fn pid(&self) -> u32;

    /// Send a frame on the task channel.
    async fn send_task(&mut self, msg: RpcMessage) -> Result<()>;

    /// Receive a frame from the task channel. Returns `None` on EOF.
    async fn recv_task(&mut self) -> Result<Option<RpcMessage>>;

    /// Send a frame on the control channel.
    async fn send_control(&mut self, msg: RpcMessage) -> Result<()>;

    /// Receive a frame from the control channel. Returns `None` on EOF.
    async fn recv_control(&mut self) -> Result<Option<RpcMessage>>;

    /// Request the worker to terminate. Implementations send SIGTERM, then
    /// after a grace period SIGKILL, then `waitpid`.
    async fn terminate(&mut self) -> Result<()>;
}

/// Spawns workers per a runtime-specific strategy.
#[async_trait]
pub trait Runtime: Send + Sync + 'static {
    /// Spawn a single worker and return a handle.
    ///
    /// The handle's task and control sockets must be ready: a subsequent
    /// `recv_control` is expected to yield `control.ready` within the boot
    /// timeout (enforced by the pool, not the runtime).
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>>;
}

// --- MockRuntime: in-memory runtime for tests ---

/// In-memory runtime used in tests. Each spawned worker is a `MockWorker`
/// that returns canned responses for any task-channel request.
pub struct MockRuntime {
    responder: std::sync::Arc<dyn Fn(&RpcMessage) -> RpcMessage + Send + Sync>,
    next_pid: std::sync::atomic::AtomicU32,
}

impl MockRuntime {
    /// Create a mock runtime that echoes every request as a successful response.
    pub fn echo() -> Self {
        Self {
            responder: std::sync::Arc::new(|msg| match msg {
                RpcMessage::Request { msgid, params, .. } => {
                    RpcMessage::response_ok(*msgid, params.clone())
                },
                _ => RpcMessage::notify("mock.unsupported", rmpv::Value::Nil),
            }),
            next_pid: std::sync::atomic::AtomicU32::new(10000),
        }
    }
}

#[async_trait]
impl Runtime for MockRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        let pid = self
            .next_pid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Box::new(MockWorker {
            pid,
            responder: self.responder.clone(),
            inbound_task: Mutex::new(VecDeque::new()),
            inbound_control: Mutex::new({
                let mut q = VecDeque::new();
                // Pre-load the Ready handshake.
                q.push_back(RpcMessage::notify(
                    "control.ready",
                    rmpv::Value::Map(vec![(
                        rmpv::Value::String("pid".into()),
                        rmpv::Value::Integer(pid.into()),
                    )]),
                ));
                q
            }),
            terminated: false,
        }))
    }
}

/// In-memory worker used by `MockRuntime`.
pub struct MockWorker {
    pid: u32,
    responder: std::sync::Arc<dyn Fn(&RpcMessage) -> RpcMessage + Send + Sync>,
    inbound_task: Mutex<VecDeque<RpcMessage>>,
    inbound_control: Mutex<VecDeque<RpcMessage>>,
    terminated: bool,
}

#[async_trait]
impl WorkerHandle for MockWorker {
    fn pid(&self) -> u32 {
        self.pid
    }

    async fn send_task(&mut self, msg: RpcMessage) -> Result<()> {
        if self.terminated {
            anyhow::bail!("worker terminated");
        }
        let response = (self.responder)(&msg);
        self.inbound_task.lock().unwrap().push_back(response);
        Ok(())
    }

    async fn recv_task(&mut self) -> Result<Option<RpcMessage>> {
        Ok(self.inbound_task.lock().unwrap().pop_front())
    }

    async fn send_control(&mut self, _msg: RpcMessage) -> Result<()> {
        Ok(())
    }

    async fn recv_control(&mut self) -> Result<Option<RpcMessage>> {
        Ok(self.inbound_control.lock().unwrap().pop_front())
    }

    async fn terminate(&mut self) -> Result<()> {
        self.terminated = true;
        Ok(())
    }
}
