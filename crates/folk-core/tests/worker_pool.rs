use std::sync::Arc;

use folk_api::Executor;
use folk_core::config::WorkersConfig;
use folk_core::runtime::MockRuntime;
use folk_core::worker_pool::WorkerPool;
use serde_json::json;

#[tokio::test]
async fn execute_round_trips_through_mock_runtime() {
    let rt = Arc::new(MockRuntime::echo());
    let config = WorkersConfig {
        count: 2,
        ..WorkersConfig::default()
    };

    let pool = WorkerPool::new(rt, config).unwrap();

    // Wait a bit for workers to boot
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let payload = json!({"msg": "hello"});
    let response = pool
        .execute_value("dispatch", payload.clone())
        .await
        .unwrap();
    assert_eq!(response, payload);
}
