//! Folk PHP extension core — server lifecycle + worker bridge.
//!
//! Uses `std::sync` channels for worker communication (no tokio dependency
//! on the worker thread side). Supports multi-worker via ZTS threads.

pub mod bridge;
pub mod registry;
pub mod runtime;
pub mod worker;
pub mod zts;
pub mod zval_convert;

use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;

use ext_php_rs::binary::Binary;
use ext_php_rs::prelude::*;
use folk_api::Plugin;
use folk_core::config::FolkConfig;
use tracing::info;

use crate::registry::InProcessRegistry;
use crate::runtime::{ExtensionRuntime, WorkerTxSide};

pub use folk_core;

static REGISTRY: OnceLock<Arc<InProcessRegistry>> = OnceLock::new();
static TOKIO_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();
static PROJECT_ROOT: OnceLock<std::path::PathBuf> = OnceLock::new();

/// Returns the project root directory (CWD at server start).
pub fn project_root() -> Option<&'static std::path::Path> {
    PROJECT_ROOT.get().map(std::path::PathBuf::as_path)
}

/// ZTS worker thread handles — joined on shutdown to prevent SIGSEGV.
static ZTS_WORKERS: OnceLock<Mutex<Vec<thread::JoinHandle<()>>>> = OnceLock::new();

/// Register a ZTS worker thread handle for graceful shutdown.
pub fn register_zts_worker(handle: thread::JoinHandle<()>) {
    let workers = ZTS_WORKERS.get_or_init(|| Mutex::new(Vec::new()));
    workers.lock().unwrap().push(handle);
}

/// Join all ZTS worker threads. Called before main thread exits.
pub fn join_zts_workers() {
    if let Some(workers) = ZTS_WORKERS.get() {
        let handles: Vec<_> = workers.lock().unwrap().drain(..).collect();
        for handle in handles {
            let _ = handle.join();
        }
        tracing::info!("all ZTS worker threads joined");
    }
}

// --- Public Rust API ---

pub fn version() -> String {
    format!("folk-ext {}", env!("CARGO_PKG_VERSION"))
}

/// Start the server with plugins. Non-blocking.
///
/// Creates one channel pair for the main PHP thread (worker #1).
/// Additional workers (count > 1) are spawned as ZTS threads by the runtime.
pub fn start_server(config: FolkConfig, plugins: Vec<Box<dyn Plugin>>) -> anyhow::Result<()> {
    // Save project root (CWD at server start) for ZTS worker threads.
    let _ = PROJECT_ROOT.set(std::env::current_dir().unwrap_or_default());

    let worker_count = config.workers.count;
    let is_zts = zts::is_zts();

    if worker_count > 1 && !is_zts {
        tracing::warn!(
            worker_count,
            "multi-worker requested but PHP is NTS; only 1 worker will be used"
        );
    }

    // Create one channel pair for the main thread worker.
    let (task_tx, task_rx) = std::sync::mpsc::sync_channel::<bridge::TaskRequest>(8);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(1);

    // Install main thread as worker #1.
    bridge::init_worker_state(1, task_rx, ready_tx);

    // Tx side goes to the runtime (for dispatching to main thread worker).
    let tx_sides = vec![WorkerTxSide { task_tx, ready_rx }];

    let registry = InProcessRegistry::new();
    REGISTRY.set(registry.clone()).ok();

    let workers_config = config.workers.clone();
    let barrier = Arc::new(Barrier::new(2));
    let barrier_inner = barrier.clone();

    thread::Builder::new()
        .name("folk-tokio".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            TOKIO_HANDLE.set(rt.handle().clone()).ok();
            barrier_inner.wait();

            rt.block_on(async move {
                // Runtime gets the pre-connected channel for worker #1.
                // Additional workers (ZTS) will be spawned on demand.
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

    barrier.wait();
    info!(
        worker_count,
        is_zts, "folk server started, main process is worker #1"
    );
    Ok(())
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
#[allow(clippy::needless_pass_by_value)] // ext-php-rs requires owned types
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
    bridge::do_send(&data).map_err(|e| PhpException::default(format!("folk_worker_send: {e}")))
}

#[cfg(feature = "standalone")]
#[php_function]
#[allow(clippy::needless_pass_by_value)] // ext-php-rs requires owned types
pub fn folk_worker_send_error(message: String) -> PhpResult<()> {
    bridge::do_send_error(&message)
        .map_err(|e| PhpException::default(format!("folk_worker_send_error: {e}")))
}

/// Returns true if the current thread is a ZTS worker thread
/// (has bridge state initialized by the runtime).
#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_is_worker_thread() -> bool {
    bridge::has_worker_state()
}

/// Returns the id of the request currently being handled (`""` if none).
///
/// A UUID v7, stable for the duration of a single request — useful for
/// correlating PHP application logs with Rust-side access logs.
#[cfg(feature = "standalone")]
#[php_function]
pub fn folk_request_id() -> String {
    bridge::current_request_id()
        .map(|id| id.to_string())
        .unwrap_or_default()
}

/// Run the zero-copy dispatch loop.
///
/// Blocks until the channel is closed (server shutdown). Calls the named
/// PHP function directly for each request — no JSON encode/decode.
///
/// The PHP function must have signature:
/// `function(string $method, array $params): array`
#[cfg(feature = "standalone")]
#[php_function]
#[allow(clippy::needless_pass_by_value)]
pub fn folk_worker_run(dispatch_fn: String) -> PhpResult<()> {
    bridge::run_dispatch_loop(&dispatch_fn)
        .map_err(|e| PhpException::default(format!("folk_worker_run: {e}")))
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
        .function(wrap_function!(folk_is_worker_thread))
        .function(wrap_function!(folk_request_id))
        .function(wrap_function!(folk_worker_run))
}
