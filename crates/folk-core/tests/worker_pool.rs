use std::sync::Arc;
use std::time::Duration;

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
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = json!({"msg": "hello"});
    let response = pool
        .execute_value("dispatch", payload.clone())
        .await
        .unwrap();
    assert_eq!(response, payload);
}

#[tokio::test]
async fn dispatch_assigns_unique_uuid_request_ids() {
    let rt = Arc::new(MockRuntime::echo());
    let config = WorkersConfig {
        count: 2,
        ..WorkersConfig::default()
    };
    let pool = WorkerPool::new(rt.clone(), config).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    for _ in 0..5 {
        pool.execute_value("dispatch", json!({})).await.unwrap();
    }

    let ids = rt.seen_request_ids();
    assert_eq!(ids.len(), 5);
    // All ids are distinct.
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), 5, "request ids must be unique");
    // Each id is a valid UUID v7.
    for id in &ids {
        let parsed = uuid::Uuid::parse_str(id).expect("request id must be a valid UUID");
        assert_eq!(parsed.get_version_num(), 7, "request id must be UUID v7");
    }
}

#[tokio::test]
async fn execute_value_traced_returns_the_dispatched_id() {
    let rt = Arc::new(MockRuntime::echo());
    let pool = WorkerPool::new(rt.clone(), WorkersConfig::default()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (_value, id) = pool
        .execute_value_traced("dispatch", json!({}))
        .await
        .unwrap();

    // The id returned to the caller is exactly the one the worker observed.
    assert_eq!(rt.seen_request_ids(), vec![id.to_string()]);
}

#[tokio::test]
async fn trigger_reload_recycles_idle_workers() {
    let rt = Arc::new(MockRuntime::echo());
    let config = WorkersConfig {
        count: 3,
        ..WorkersConfig::default()
    };

    let pool = WorkerPool::new(rt.clone(), config).unwrap();

    // Wait for the initial 3 workers to boot.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(rt.spawn_count(), 3, "expected 3 workers booted");

    // Reload: all 3 (recyclable) workers should restart even while idle.
    pool.trigger_reload().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(rt.spawn_count(), 6, "expected 3 fresh workers after reload");

    // Pool still serves requests after the reload.
    let payload = json!({"msg": "after-reload"});
    let response = pool
        .execute_value("dispatch", payload.clone())
        .await
        .unwrap();
    assert_eq!(response, payload);
}
