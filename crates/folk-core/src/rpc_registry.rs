//! Concrete `RpcRegistrar` impl for `folk-api`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use folk_api::{RpcHandler, RpcRegistrar};
use tokio::sync::RwLock;

/// In-memory RPC method registry.
pub struct RpcRegistry {
    methods: RwLock<HashMap<String, RpcHandler>>,
}

impl RpcRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            methods: RwLock::new(HashMap::new()),
        })
    }

    /// Look up a handler by method name.
    pub async fn get(&self, name: &str) -> Option<RpcHandler> {
        self.methods.read().await.get(name).cloned()
    }

    /// Snapshot of registered method names (for `folk admin list-methods`).
    pub async fn list(&self) -> Vec<String> {
        self.methods.read().await.keys().cloned().collect()
    }

    /// Dispatch a request: look up the handler, call it, return the response.
    pub async fn dispatch(&self, method: &str, request: Bytes) -> anyhow::Result<Bytes> {
        match self.get(method).await {
            Some(handler) => handler(request).await,
            None => anyhow::bail!("method not found: {method}"),
        }
    }
}

#[async_trait]
impl RpcRegistrar for RpcRegistry {
    async fn register_raw(&self, name: String, handler: RpcHandler) {
        self.methods.write().await.insert(name, handler);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn registers_and_dispatches() {
        let reg = RpcRegistry::new();
        reg.register_raw(
            "echo".into(),
            Arc::new(|req: Bytes| Box::pin(async move { Ok(req) })),
        )
        .await;

        let resp = reg
            .dispatch("echo", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        assert_eq!(resp, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn dispatch_unknown_method_errors() {
        let reg = RpcRegistry::new();
        let result = reg.dispatch("nope", Bytes::new()).await;
        assert!(result.is_err());
    }
}
