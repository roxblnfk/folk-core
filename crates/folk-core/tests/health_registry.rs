use std::sync::Arc;

use folk_api::{HealthRegistry, HealthStatus};
use folk_core::health_registry::HealthRegistryImpl;

#[tokio::test]
async fn registers_and_aggregates() {
    let reg = HealthRegistryImpl::new();
    reg.register(
        "plugin-a".into(),
        Arc::new(|| Box::pin(async { HealthStatus::ok() })),
    )
    .await;
    reg.register(
        "plugin-b".into(),
        Arc::new(|| Box::pin(async { HealthStatus::degraded("queue full") })),
    )
    .await;

    let results = reg.check_all().await;
    assert_eq!(results.len(), 2);
    assert!(results["plugin-a"].healthy);
    assert!(!results["plugin-b"].healthy);
}
