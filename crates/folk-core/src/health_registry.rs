//! Concrete `HealthRegistry` impl.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use folk_api::{HealthCheckFn, HealthRegistry, HealthStatus};
use futures_util::future::join_all;
use tokio::sync::RwLock;

pub struct HealthRegistryImpl {
    checks: RwLock<HashMap<String, HealthCheckFn>>,
}

impl HealthRegistryImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            checks: RwLock::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl HealthRegistry for HealthRegistryImpl {
    async fn register(&self, plugin_name: String, check: HealthCheckFn) {
        self.checks.write().await.insert(plugin_name, check);
    }

    async fn check_all(&self) -> HashMap<String, HealthStatus> {
        let snapshot: Vec<(String, HealthCheckFn)> = {
            let guard = self.checks.read().await;
            guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        let futures = snapshot.into_iter().map(|(name, check)| async move {
            let status = check().await;
            (name, status)
        });

        join_all(futures).await.into_iter().collect()
    }
}
