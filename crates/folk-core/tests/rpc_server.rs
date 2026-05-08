use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use folk_api::RpcRegistrar;
use folk_core::rpc_registry::RpcRegistry;
use folk_core::rpc_server::run_rpc_server;
use folk_protocol::{FrameCodec, RpcMessage};
use futures_util::{SinkExt, StreamExt};
use rmpv::Value;
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio_util::codec::Framed;

#[tokio::test]
async fn rpc_server_dispatches_request_to_handler() {
    let dir = TempDir::new().unwrap();
    let sock = dir.path().join("test.sock");

    let registry = RpcRegistry::new();
    registry
        .register_raw(
            "ping".into(),
            Arc::new(|_: Bytes| {
                Box::pin(async move {
                    let v = Value::String("pong".into());
                    Ok(Bytes::from(rmp_serde::to_vec(&v).unwrap()))
                })
            }),
        )
        .await;

    let (sd_tx, sd_rx) = watch::channel(false);
    let sock_path = sock.clone();
    let reg_clone = registry.clone();
    tokio::spawn(async move {
        run_rpc_server(&sock_path, reg_clone, sd_rx).await.unwrap();
    });

    // Wait for socket to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    let stream = UnixStream::connect(&sock).await.unwrap();
    let mut framed = Framed::new(stream, FrameCodec::new());

    framed
        .send(RpcMessage::request(1, "ping", Value::Nil))
        .await
        .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    match response {
        RpcMessage::Response { msgid, result, .. } => {
            assert_eq!(msgid, 1);
            assert_eq!(result.as_str(), Some("pong"));
        },
        other => panic!("unexpected: {other:?}"),
    }

    sd_tx.send(true).unwrap();
}
