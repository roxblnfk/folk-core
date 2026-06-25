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

    // MockRuntime echoes the payload back. execute_value goes through the
    // streaming path: value_to_chunks → ResponseChunk stream → collect_stream.
    // The PHP response convention is {status, headers, body}; values missing
    // those keys get defaults (200, {}, "").
    let payload = json!({"status": 200, "headers": {"X-Test": "ok"}, "body": "hello"});
    let response = pool
        .execute_value("dispatch", payload.clone())
        .await
        .unwrap();
    assert_eq!(response["status"], 200);
    assert_eq!(response["headers"]["X-Test"], "ok");
    assert_eq!(response["body"], "hello");
}
