use folk_core::runtime::{MockRuntime, Runtime};
use serde_json::json;

#[tokio::test]
async fn mock_runtime_echoes_request() {
    let rt = MockRuntime::echo();
    let mut worker = rt.spawn().await.unwrap();

    // Ready should succeed immediately for MockRuntime.
    worker.ready().await.unwrap();

    // Execute should echo the payload back.
    let payload = json!({"hello": "world"});
    let result = worker.execute("test", payload.clone(), 1).await.unwrap();
    assert_eq!(result, payload);
}

#[tokio::test]
async fn mock_runtime_warmup_is_noop() {
    let rt = MockRuntime::echo();
    rt.warmup().await.unwrap();
}

#[tokio::test]
async fn mock_worker_refuses_after_terminate() {
    let rt = MockRuntime::echo();
    let mut worker = rt.spawn().await.unwrap();
    worker.ready().await.unwrap();
    worker.terminate().await.unwrap();

    let result = worker.execute("test", json!("hi"), 1).await;
    assert!(result.is_err());
}
