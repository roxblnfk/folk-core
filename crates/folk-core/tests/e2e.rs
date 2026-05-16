//! End-to-end integration test for `folk-core` with `MockRuntime`.

use std::sync::Arc;
use std::time::Duration;

use folk_api::Executor;
use folk_core::config::WorkersConfig;
use folk_core::runtime::MockRuntime;
use folk_core::worker_pool::WorkerPool;
use serde_json::json;

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

    let payload = json!({"msg": "hello"});
    let response = pool
        .execute_value("dispatch", payload.clone())
        .await
        .unwrap();
    assert_eq!(response, payload);
}
