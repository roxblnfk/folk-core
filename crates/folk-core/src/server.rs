//! `FolkServer`: the lifecycle owner for the Folk application server.
//!
//! Constructed by `folk-ext` (PHP extension) or in tests with `MockRuntime`.

use std::sync::Arc;

use anyhow::{Context, Result};
use folk_api::{Plugin, PluginContext, RpcRegistrar};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::FolkConfig;
use crate::health_registry::HealthRegistryImpl;
use crate::logging;
use crate::metrics_registry::MetricsRegistryImpl;
use crate::plugin_registry::PluginRegistry;
use crate::runtime::Runtime;
use crate::worker_pool::WorkerPool;

/// The Folk application server.
pub struct FolkServer {
    config: FolkConfig,
    runtime: Arc<dyn Runtime>,
    plugins: PluginRegistry,
    rpc_registrar: Option<Arc<dyn RpcRegistrar>>,
}

impl FolkServer {
    /// Construct a server with the given config and runtime.
    pub fn new(config: FolkConfig, runtime: Arc<dyn Runtime>) -> Self {
        Self {
            config,
            runtime,
            plugins: PluginRegistry::new(),
            rpc_registrar: None,
        }
    }

    /// Set an in-process RPC registrar for plugin method registration.
    /// Plugins will register their handlers here during boot.
    pub fn set_rpc_registrar(&mut self, registrar: Arc<dyn RpcRegistrar>) {
        self.rpc_registrar = Some(registrar);
    }

    /// Register a plugin. Call before `run`.
    pub fn register_plugin(&mut self, plugin: Box<dyn Plugin>) {
        self.plugins.register(plugin);
    }

    /// Run the server until SIGTERM/SIGINT.
    pub async fn run(mut self) -> Result<()> {
        let _ = logging::init(&self.config.log);

        self.config.workers.normalize();

        info!(
            version = folk_api::FOLK_API_VERSION,
            workers = self.config.workers.count,
            "folk server starting"
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let health_registry = HealthRegistryImpl::new();
        let metrics_registry = MetricsRegistryImpl::new();

        if self.config.workers.warmup {
            match self.runtime.warmup().await {
                Ok(()) => {},
                Err(e) => warn!(error = %e, "opcache warmup failed, skipping"),
            }
        }

        let pool = WorkerPool::new(self.runtime.clone(), self.config.workers.clone())
            .context("failed to start worker pool")?;

        info!("worker pool started");

        // Dev-mode hot reload: watch PHP files and recycle workers on change.
        // Keep the guard alive for the lifetime of the server.
        let _watch_guard = if self.config.dev.watch {
            if self.config.workers.count <= 1 {
                warn!(
                    "hot reload enabled with workers.count <= 1: the main PHP thread is not \
                     recyclable, so code changes will not be picked up. Set workers.count > 1."
                );
            }
            match crate::watch::spawn(&self.config.dev, pool.clone()) {
                Ok(guard) => Some(guard),
                Err(e) => {
                    warn!(error = %e, "failed to start hot reload watcher; continuing without it");
                    None
                },
            }
        } else {
            None
        };

        let ctx = PluginContext {
            executor: pool.clone(),
            shutdown: shutdown_rx.clone(),
            rpc_registrar: self.rpc_registrar.clone(),
            health_registry: Some(health_registry.clone()),
            metrics_registry: Some(metrics_registry.clone()),
        };

        self.plugins
            .boot_all(&ctx)
            .await
            .context("plugin boot failed")?;

        info!(plugins = ?self.plugins.names(), "all plugins booted");

        // Wait for shutdown signal.
        wait_for_signal().await;
        info!("shutdown signal received; draining");

        let _ = shutdown_tx.send(true);

        let timeout = self.config.server.shutdown_timeout;
        let shutdown_result =
            tokio::time::timeout(timeout, async { self.plugins.shutdown_all().await }).await;

        if shutdown_result.is_err() {
            warn!(?timeout, "graceful shutdown timed out; forcing");
        }

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
