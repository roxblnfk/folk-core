//! Folk PHP extension core — server lifecycle + worker bridge.
//!
//! This crate provides:
//! - `start_server()` — start tokio in background, main thread = worker
//! - `bridge` — worker channel communication (recv/send)
//! - `registry` — in-process plugin method registry + `call()`
//!
//! PHP wrappers (#[php_class], #[php_function], #[php_module]) are either
//! in this crate's own cdylib output, or in folk-builder generated code.

pub mod bridge;
pub mod registry;
pub mod runtime;
pub mod worker;

use std::sync::{Arc, OnceLock};
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

/// Global registry for plugin method calls.
static REGISTRY: OnceLock<Arc<InProcessRegistry>> = OnceLock::new();

/// Tokio handle for blocking calls from PHP thread.
static TOKIO_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();

// --- Public Rust API ---

/// Return the extension version string.
pub fn version() -> String {
    format!("folk-ext {}", env!("CARGO_PKG_VERSION"))
}

/// Start the server with plugins. Non-blocking.
///
/// Creates channels for the main thread worker, starts tokio in a background
/// thread, returns immediately. The caller should then enter the worker loop
/// via `bridge::do_ready()`, `bridge::do_recv()`, `bridge::do_send()`.
pub fn start_server(config: FolkConfig, plugins: Vec<Box<dyn Plugin>>) -> anyhow::Result<()> {
    let (task_tx, task_rx) = mpsc::channel::<bridge::TaskRequest>(8);
    let (ready_tx, ready_rx) = oneshot::channel::<()>();

    bridge::init_worker_state(1, task_rx, ready_tx);

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
                let ext_runtime =
                    Arc::new(ExtensionRuntime::new(workers_config, task_tx, ready_rx));

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
    info!("folk server started in background, main thread is the worker");
    Ok(())
}

/// Call a registered plugin method (in-process). Blocks until result.
pub fn call_method(method: &str, payload: bytes::Bytes) -> anyhow::Result<bytes::Bytes> {
    let registry = REGISTRY
        .get()
        .ok_or_else(|| anyhow::anyhow!("server not started"))?;
    let handle = TOKIO_HANDLE
        .get()
        .ok_or_else(|| anyhow::anyhow!("runtime not available"))?;

    handle.block_on(registry.call(method, payload))
}

// --- PHP wrappers (standalone mode only — when folk-ext IS the extension) ---
// When used as rlib by folk-builder, these are NOT compiled to avoid
// duplicate `get_module` symbols.

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
}
