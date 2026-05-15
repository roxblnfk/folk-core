//! End-to-end integration test for `folk-core` with `MockRuntime`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use folk_api::Executor;
use folk_core::config::WorkersConfig;
use folk_core::runtime::MockRuntime;
use folk_core::worker_pool::WorkerPool;

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
