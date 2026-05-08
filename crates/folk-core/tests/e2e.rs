//! End-to-end integration test for `folk-core` with `MockRuntime`.
//!
//! Does NOT test PHP processes; that's phase 4+. Tests server lifecycle.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use folk_api::{Executor, Plugin, PluginContext, RpcMethodDef};
use folk_core::config::WorkersConfig;
use folk_core::health_registry::HealthRegistryImpl;
use folk_core::metrics_registry::MetricsRegistryImpl;
use folk_core::rpc_registry::RpcRegistry;
use folk_core::runtime::MockRuntime;
use folk_core::worker_pool::WorkerPool;
use rmpv::Value;
use tokio::sync::watch;

// --- A minimal plugin that registers one RPC method ---

struct PingPlugin;

#[async_trait]
impl Plugin for PingPlugin {
    fn name(&self) -> &'static str {
        "ping"
    }

    async fn boot(&mut self, ctx: PluginContext) -> Result<()> {
        if let Some(reg) = &ctx.rpc_registrar {
            reg.register_raw(
                "ping".into(),
                Arc::new(|_: Bytes| {
                    Box::pin(async move {
                        let v = Value::String("pong".into());
                        Ok(Bytes::from(rmp_serde::to_vec(&v).unwrap()))
                    })
                }),
            )
            .await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn rpc_methods(&self) -> Vec<RpcMethodDef> {
        vec![RpcMethodDef::new("ping", "echo pong")]
    }
}

// --- Tests ---

#[tokio::test]
async fn executor_round_trip_with_mock_runtime() {
    let runtime = Arc::new(MockRuntime::echo());
    let config = WorkersConfig {
        count: 1,
        boot_timeout: Duration::from_secs(5),
        ..WorkersConfig::default()
    };

    let pool = WorkerPool::new(runtime, config).unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = rmp_serde::to_vec(&"hello").unwrap();
    let response = pool.execute(Bytes::from(payload)).await.unwrap();
    let decoded: String = rmp_serde::from_slice(&response).unwrap();
    assert_eq!(decoded, "hello");
}

#[tokio::test]
async fn plugin_rpc_method_is_reachable_via_rpc_registry() {
    let runtime = Arc::new(MockRuntime::echo());
    let config = WorkersConfig {
        count: 1,
        boot_timeout: Duration::from_secs(5),
        ..WorkersConfig::default()
    };

    let pool = WorkerPool::new(runtime, config).unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify the pool dispatches via executor
    let payload = rmp_serde::to_vec(&"hello").unwrap();
    let response = pool.execute(Bytes::from(payload)).await.unwrap();
    let decoded: String = rmp_serde::from_slice(&response).unwrap();
    assert_eq!(decoded, "hello");

    let rpc_registry = RpcRegistry::new();
    let health_registry = HealthRegistryImpl::new();
    let metrics_registry = MetricsRegistryImpl::new();

    let (_, rx) = watch::channel(false);
    let ctx = PluginContext {
        executor: pool.clone(),
        shutdown: rx,
        rpc_registrar: Some(rpc_registry.clone()),
        health_registry: Some(health_registry.clone()),
        metrics_registry: Some(metrics_registry.clone()),
    };

    let mut plugin = PingPlugin;
    plugin.boot(ctx).await.unwrap();

    // Verify the method was registered
    assert!(rpc_registry.get("ping").await.is_some());
    let methods = rpc_registry.list().await;
    assert!(methods.contains(&"ping".to_string()));
}
