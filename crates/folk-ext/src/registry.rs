//! In-process plugin method registry.
//!
//! Replaces the old Unix-socket RPC registry. Plugins register handlers
//! during boot (same API), and PHP code calls them via `folk_call()`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use folk_api::{RpcHandler, RpcRegistrar};
use tokio::sync::RwLock;
use tracing::debug;

/// In-process registry that stores plugin method handlers.
/// Accessible from PHP via `folk_call()`.
#[derive(Clone)]
pub struct InProcessRegistry {
    handlers: Arc<RwLock<HashMap<String, RpcHandler>>>,
}

impl InProcessRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Call a registered method. Used by the PHP bridge.
    pub async fn call(&self, method: &str, payload: Bytes) -> Result<Bytes> {
        let handlers = self.handlers.read().await;
        let handler = handlers
            .get(method)
            .ok_or_else(|| anyhow::anyhow!("method not found: {method}"))?
            .clone();
        drop(handlers);

        handler(payload).await
    }

    /// List registered methods.
    pub async fn methods(&self) -> Vec<String> {
        self.handlers.read().await.keys().cloned().collect()
    }
}

#[async_trait]
impl RpcRegistrar for InProcessRegistry {
    async fn register_raw(&self, name: String, handler: RpcHandler) {
        debug!(method = %name, "registered plugin method");
        self.handlers.write().await.insert(name, handler);
    }
}
