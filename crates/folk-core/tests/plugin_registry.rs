use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use folk_api::{Executor, Plugin, PluginContext};
use folk_core::plugin_registry::PluginRegistry;
use tokio::sync::watch;

static SEQ: AtomicUsize = AtomicUsize::new(0);

struct TrackingPlugin {
    name: &'static str,
    boot_order: Arc<AtomicUsize>,
    shutdown_order: Arc<AtomicUsize>,
}

#[async_trait]
impl Plugin for TrackingPlugin {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn boot(&mut self, _: PluginContext) -> Result<()> {
        self.boot_order
            .store(SEQ.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.shutdown_order
            .store(SEQ.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
        Ok(())
    }
}

struct StubExecutor;
#[async_trait]
impl Executor for StubExecutor {
    async fn execute(&self, _: Bytes) -> Result<Bytes> {
        unimplemented!()
    }
}

#[tokio::test]
async fn boots_in_order_and_shuts_in_reverse() {
    SEQ.store(0, Ordering::SeqCst);
    let a_b = Arc::new(AtomicUsize::new(0));
    let a_s = Arc::new(AtomicUsize::new(0));
    let b_b = Arc::new(AtomicUsize::new(0));
    let b_s = Arc::new(AtomicUsize::new(0));

    let mut reg = PluginRegistry::new();
    reg.register(Box::new(TrackingPlugin {
        name: "a",
        boot_order: a_b.clone(),
        shutdown_order: a_s.clone(),
    }));
    reg.register(Box::new(TrackingPlugin {
        name: "b",
        boot_order: b_b.clone(),
        shutdown_order: b_s.clone(),
    }));

    let (_tx, rx) = watch::channel(false);
    let ctx = PluginContext {
        executor: Arc::new(StubExecutor),
        shutdown: rx,
        rpc_registrar: None,
        health_registry: None,
        metrics_registry: None,
    };

    reg.boot_all(&ctx).await.unwrap();
    assert!(a_b.load(Ordering::SeqCst) < b_b.load(Ordering::SeqCst));

    reg.shutdown_all().await;
    assert!(b_s.load(Ordering::SeqCst) < a_s.load(Ordering::SeqCst));
}
