//! `FolkServer`: the lifecycle owner for the Folk application server.
//!
//! Typically constructed by the `folk` binary (phase 5) or by `folk-builder`-
//! generated binaries. In tests, use `FolkServer::new(config, mock_runtime)`.

use std::sync::Arc;

use anyhow::{Context, Result};
use folk_api::{Plugin, PluginContext};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::FolkConfig;
use crate::health_registry::HealthRegistryImpl;
use crate::logging;
use crate::metrics_registry::MetricsRegistryImpl;
use crate::plugin_registry::PluginRegistry;
use crate::rpc_registry::RpcRegistry;
use crate::rpc_server;
use crate::runtime::Runtime;
use crate::worker_pool::WorkerPool;

/// The Folk application server.
pub struct FolkServer {
    config: FolkConfig,
    runtime: Arc<dyn Runtime>,
    plugins: PluginRegistry,
}

impl FolkServer {
    /// Construct a server with the given config and runtime.
    ///
    /// In production, the runtime is `folk_runtime_pipe::PipeRuntime`
    /// (phase 4). For tests, pass `MockRuntime::echo()`.
    pub fn new(config: FolkConfig, runtime: Arc<dyn Runtime>) -> Self {
        Self {
            config,
            runtime,
            plugins: PluginRegistry::new(),
        }
    }

    /// Register a plugin. Call before `run`.
    pub fn register_plugin(&mut self, plugin: Box<dyn Plugin>) {
        self.plugins.register(plugin);
    }

    /// Run the server until SIGTERM (or SIGINT on dev builds).
    ///
    /// This method:
    /// 1. Initializes logging.
    /// 2. Spawns the worker pool.
    /// 3. Boots all registered plugins.
    /// 4. Starts the admin RPC server.
    /// 5. Waits for SIGTERM.
    /// 6. Gracefully shuts down: RPC server, plugins (in reverse), pool.
    /// 7. Returns `Ok(())` on clean exit.
    pub async fn run(mut self) -> Result<()> {
        // 1. Logging.
        let _ = logging::init(&self.config.log); // ignore reinit errors in tests

        info!(
            version = folk_api::FOLK_API_VERSION,
            workers = self.config.workers.count,
            runtime = ?self.config.server.runtime,
            "folk server starting"
        );

        // 2. Shutdown broadcast.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // 3. Registries.
        let rpc_registry = RpcRegistry::new();
        let health_registry = HealthRegistryImpl::new();
        let metrics_registry = MetricsRegistryImpl::new();

        // 4. Worker pool.
        let pool = WorkerPool::new(self.runtime.clone(), self.config.workers.clone())
            .context("failed to start worker pool")?;

        info!("worker pool started");

        // 5. Plugin context.
        let ctx = PluginContext {
            executor: pool.clone(),
            shutdown: shutdown_rx.clone(),
            rpc_registrar: Some(rpc_registry.clone()),
            health_registry: Some(health_registry.clone()),
            metrics_registry: Some(metrics_registry.clone()),
        };

        // 6. Boot plugins.
        self.plugins
            .boot_all(&ctx)
            .await
            .context("plugin boot failed")?;

        info!(plugins = ?self.plugins.names(), "all plugins booted");

        // 7. Start admin RPC server.
        let rpc_path = self.config.server.rpc_socket.clone();
        let rpc_reg = rpc_registry.clone();
        let rpc_sd = shutdown_rx.clone();
        let rpc_task = tokio::spawn(async move {
            if let Err(e) = rpc_server::run_rpc_server(&rpc_path, rpc_reg, rpc_sd).await {
                warn!(error = ?e, "admin RPC server error");
            }
        });

        // 8. Wait for shutdown signal.
        wait_for_signal().await;
        info!("shutdown signal received; draining");

        // 9. Signal all components.
        let _ = shutdown_tx.send(true);

        // 10. Shutdown timeout.
        let timeout = self.config.server.shutdown_timeout;
        let shutdown_result =
            tokio::time::timeout(timeout, async { self.plugins.shutdown_all().await }).await;

        if shutdown_result.is_err() {
            warn!(?timeout, "graceful shutdown timed out; forcing");
        }

        rpc_task.abort();
        info!("folk server stopped");
        Ok(())
    }
}

#[cfg(not(target_os = "windows"))]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => { info!("received SIGTERM"); },
        _ = sigint.recv() => { info!("received SIGINT"); },
    }
}

#[cfg(target_os = "windows")]
async fn wait_for_signal() {
    tokio::signal::ctrl_c().await.expect("ctrl-c");
}
