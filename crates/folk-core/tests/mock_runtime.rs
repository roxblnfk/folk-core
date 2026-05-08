use folk_core::runtime::{MockRuntime, Runtime};
use folk_protocol::RpcMessage;
use rmpv::Value;

#[tokio::test]
async fn mock_runtime_echoes_request() {
    let rt = MockRuntime::echo();
    let mut worker = rt.spawn().await.unwrap();

    // Drain the pre-loaded Ready frame
    let ready = worker.recv_control().await.unwrap().unwrap();
    if let RpcMessage::Notify { method, .. } = ready {
        assert_eq!(method, "control.ready");
    } else {
        panic!("expected Ready notify");
    }

    let req = RpcMessage::request(1, "test", Value::String("hello".into()));
    worker.send_task(req).await.unwrap();
    let resp = worker.recv_task().await.unwrap().unwrap();

    if let RpcMessage::Response {
        msgid,
        error,
        result,
    } = resp
    {
        assert_eq!(msgid, 1);
        assert!(error.is_nil());
        assert_eq!(result.as_str(), Some("hello"));
    } else {
        panic!("expected Response");
    }
}
