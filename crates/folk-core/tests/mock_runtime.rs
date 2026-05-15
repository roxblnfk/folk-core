use bytes::Bytes;
use folk_core::runtime::{MockRuntime, Runtime};

#[tokio::test]
async fn mock_runtime_echoes_request() {
    let rt = MockRuntime::echo();
    let mut worker = rt.spawn().await.unwrap();

    // Ready should succeed immediately for MockRuntime.
    worker.ready().await.unwrap();

    // Execute should echo the payload back.
    let payload = Bytes::from_static(b"hello");
    let result = worker.execute("test", payload.clone()).await.unwrap();
    assert_eq!(result, payload);
}

#[tokio::test]
async fn mock_worker_refuses_after_terminate() {
    let rt = MockRuntime::echo();
    let mut worker = rt.spawn().await.unwrap();
    worker.ready().await.unwrap();
    worker.terminate().await.unwrap();

    let result = worker.execute("test", Bytes::from_static(b"hi")).await;
    assert!(result.is_err());
}
