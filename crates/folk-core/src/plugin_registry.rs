//! Plugin registry — owns the boot/shutdown ordering.

use anyhow::{Context, Result};
use folk_api::{Plugin, PluginContext};
use tracing::{error, info};

/// Holds plugins and orchestrates their boot/shutdown.
///
/// Boot order: registration order. Shutdown order: reverse.
pub struct PluginRegistry {
    plugins: Vec<Box<dyn Plugin>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }

    /// Register a plugin. Plugins are booted in registration order.
    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        info!(plugin = plugin.name(), "registered");
        self.plugins.push(plugin);
    }

    /// Boot all plugins in order. If any boot returns `Err`, shutdown is
    /// run on already-booted plugins (in reverse) and the error is propagated.
    pub async fn boot_all(&mut self, ctx: &PluginContext) -> Result<()> {
        for (booted_count, plugin) in self.plugins.iter_mut().enumerate() {
            let name = plugin.name();
            info!(plugin = name, "booting");
            if let Err(e) = plugin.boot(ctx.clone()).await {
                error!(plugin = name, error = ?e, "boot failed; rolling back");
                self.shutdown_partial(booted_count).await;
                return Err(e).with_context(|| format!("plugin '{name}' boot failed"));
            }
        }
        Ok(())
    }

    /// Shut down all plugins in reverse order. Errors are logged but do not
    /// stop the shutdown loop.
    pub async fn shutdown_all(&self) {
        self.shutdown_partial(self.plugins.len()).await;
    }

    async fn shutdown_partial(&self, count: usize) {
        for plugin in self.plugins.iter().take(count).rev() {
            let name = plugin.name();
            info!(plugin = name, "shutting down");
            if let Err(e) = plugin.shutdown().await {
                error!(plugin = name, error = ?e, "shutdown error (continuing)");
            }
        }
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.plugins.iter().map(|p| p.name()).collect()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}
