//! Folk PHP extension core — server lifecycle + worker bridge.
//!
//! Supports multi-worker via fork: start_server() creates N channel pairs,
//! PHP forks N-1 children, each process takes one channel and enters the loop.

pub mod bridge;
pub mod registry;
pub mod runtime;
pub mod worker;

use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use ext_php_rs::binary::Binary;
use ext_php_rs::prelude::*;
use folk_api::Plugin;
use folk_core::config::FolkConfig;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::registry::InProcessRegistry;
use crate::runtime::ExtensionRuntime;

pub use folk_core;

static REGISTRY: OnceLock<Arc<InProcessRegistry>> = OnceLock::new();
static TOKIO_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();

/// Pending worker channels (rx sides) for forked processes to pick up.
static PENDING_WORKERS: OnceLock<
    Mutex<Vec<(mpsc::Receiver<bridge::TaskRequest>, oneshot::Sender<()>)>>,
> = OnceLock::new();

// --- Public Rust API ---

pub fn version() -> String {
    format!("folk-ext {}", env!("CARGO_PKG_VERSION"))
}

/// Start the server with plugins. Non-blocking.
///
/// Creates `config.workers.count` channel pairs. The first worker's channels
/// are installed for the current process. Additional channels are stored in
/// PENDING_WORKERS for forked children to claim via `claim_worker_channel()`.
pub fn start_server(config: FolkConfig, plugins: Vec<Box<dyn Plugin>>) -> anyhow::Result<()> {
    let worker_count = config.workers.count;

    let mut tx_sides = Vec::with_capacity(worker_count);
    let mut rx_sides = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let (task_tx, task_rx) = mpsc::channel::<bridge::TaskRequest>(8);
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        tx_sides.push((task_tx, ready_rx));
        rx_sides.push((task_rx, ready_tx));
    }

    // First worker: current process.
    let (first_rx, first_ready_tx) = rx_sides.remove(0);
    bridge::init_worker_state(1, first_rx, first_ready_tx);

    // Remaining workers: stored for forked children.
    PENDING_WORKERS.set(Mutex::new(rx_sides)).ok();

    let registry = InProcessRegistry::new();
    REGISTRY.set(registry.clone()).ok();

    let workers_config = config.workers.clone();

    thread::Builder::new()
        .name("folk-tokio".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            TOKIO_HANDLE.set(rt.handle().clone()).ok();

            rt.block_on(async move {
                let ext_runtime = Arc::new(ExtensionRuntime::new(workers_config, tx_sides));

                let mut server = folk_core::server::FolkServer::new(config, ext_runtime);
                server.set_rpc_registrar(registry);

                for plugin in plugins {
                    server.register_plugin(plugin);
                }

                if let Err(e) = server.run().await {
                    tracing::error!(error = ?e, "server error");
                }
            });
        })?;

    std::thread::sleep(std::time::Duration::from_millis(100));
    info!(
        worker_count,
        "folk server started, main process is worker #1"
    );
    Ok(())
}

/// Claim the next pending worker channel (for forked children).
/// Returns the worker_id, or None if no more channels.
pub fn claim_worker_channel() -> Option<u32> {
    static NEXT_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(2);

    let pending = PENDING_WORKERS.get()?;
    let (rx, ready_tx) = pending.lock().ok()?.pop()?;
    let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    bridge::init_worker_state(id, rx, ready_tx);
    Some(id)
}

pub fn call_method(method: &str, payload: bytes::Bytes) -> anyhow::Result<bytes::Bytes> {
    let registry = REGISTRY
        .get()
        .ok_or_else(|| anyhow::anyhow!("server not started"))?;
    let handle = TOKIO_HANDLE
        .get()
        .ok_or_else(|| anyhow::anyhow!("runtime not available"))?;

    handle.block_on(registry.call(method, payload))
}

// --- PHP wrappers (standalone mode only) ---

#[cfg(feature = "standalone")]
#[php_class]
#[php(name = "Folk\\Server")]
#[derive(Debug)]
pub struct Server {
    config_path: String,
}

#[cfg(feature = "standalone")]
#[php_impl]
impl Server {
    pub fn __construct(config_path: String) -> Self {
        Self { config_path }
    }

    pub fn start(&self) -> PhpResult<()> {
        let config = FolkConfig::load_from(&self.config_path)
            .map_err(|e| PhpException::default(format!("Config error: {e}")))?;

        start_server(config, vec![])
            .map_err(|e| PhpException::default(format!("Start error: {e}")))?;

        Ok(())
    }
}

#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_version() -> String {
    version()
}

#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_call(method: String, payload: Binary<u8>) -> PhpResult<Binary<u8>> {
    let data: Vec<u8> = payload.into();
    let result = call_method(&method, bytes::Bytes::from(data))
        .map_err(|e| PhpException::default(format!("folk_call({method}): {e}")))?;

    Ok(Binary::new(result.to_vec()))
}

#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_worker_ready() -> PhpResult<bool> {
    bridge::do_ready().map_err(|e| PhpException::default(format!("folk_worker_ready: {e}")))
}

#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_worker_recv() -> PhpResult<Option<Vec<Binary<u8>>>> {
    match bridge::do_recv() {
        Ok(Some((method, payload))) => Ok(Some(vec![
            Binary::new(method.into_bytes()),
            Binary::new(payload),
        ])),
        Ok(None) => Ok(None),
        Err(e) => Err(PhpException::default(format!("folk_worker_recv: {e}"))),
    }
}

#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_worker_send(result: Binary<u8>) -> PhpResult<()> {
    let data: Vec<u8> = result.into();
    bridge::do_send(data).map_err(|e| PhpException::default(format!("folk_worker_send: {e}")))
}

#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_worker_send_error(message: String) -> PhpResult<()> {
    bridge::do_send_error(message)
        .map_err(|e| PhpException::default(format!("folk_worker_send_error: {e}")))
}

/// Claim a worker channel for a forked child process.
/// Returns the worker ID, or null if no more workers available.
#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_claim_worker() -> Option<i64> {
    claim_worker_channel().map(i64::from)
}

#[cfg(feature = "standalone")]
#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .class::<Server>()
        .function(wrap_function!(folk_version))
        .function(wrap_function!(folk_call))
        .function(wrap_function!(folk_worker_ready))
        .function(wrap_function!(folk_worker_recv))
        .function(wrap_function!(folk_worker_send))
        .function(wrap_function!(folk_worker_send_error))
        .function(wrap_function!(folk_claim_worker))
}
