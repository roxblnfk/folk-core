//! Admin RPC server: Unix socket listener that dispatches to registered handlers.
//!
//! Not to be confused with the master-worker task channel. This is for
//! external admin tooling (PHP's RPC client, `folk admin`, monitoring).

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use folk_protocol::{FrameCodec, RpcMessage};
use futures_util::{SinkExt, StreamExt};
use rmpv::Value;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use crate::rpc_registry::RpcRegistry;

/// Bind the admin RPC socket and serve connections until shutdown is signaled.
pub async fn run_rpc_server(
    socket_path: impl AsRef<Path>,
    registry: Arc<RpcRegistry>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let path = socket_path.as_ref();

    // Remove stale socket file.
    if path.exists() {
        std::fs::remove_file(path)?;
    }

    let listener = UnixListener::bind(path)?;
    info!(socket = %path.display(), "admin RPC server listening");

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let reg = registry.clone();
                        let sd = shutdown.clone();
                        tokio::spawn(handle_connection(stream, reg, sd));
                    },
                    Err(e) => {
                        error!(error = ?e, "accept error");
                    },
                }
            },
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("admin RPC server shutting down");
                    break;
                }
            },
        }
    }

    // Remove socket file on clean exit.
    let _ = std::fs::remove_file(path);
    Ok(())
}

async fn handle_connection(
    stream: UnixStream,
    registry: Arc<RpcRegistry>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut framed = Framed::new(stream, FrameCodec::new());

    loop {
        tokio::select! {
            frame = framed.next() => {
                match frame {
                    Some(Ok(msg)) => {
                        let response = dispatch(&registry, msg).await;
                        if let Err(e) = framed.send(response).await {
                            warn!(error = ?e, "send error; closing connection");
                            return;
                        }
                    },
                    Some(Err(e)) => {
                        warn!(error = ?e, "frame decode error; closing connection");
                        return;
                    },
                    None => {
                        debug!("connection closed by peer");
                        return;
                    },
                }
            },
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("connection closed due to shutdown");
                    return;
                }
            },
        }
    }
}

async fn dispatch(registry: &RpcRegistry, msg: RpcMessage) -> RpcMessage {
    match msg {
        RpcMessage::Request { msgid, method, .. } => {
            match registry.dispatch(&method, bytes::Bytes::new()).await {
                Ok(bytes) => {
                    let result = rmp_serde::from_slice::<Value>(&bytes)
                        .unwrap_or(Value::String(format!("{bytes:?}").into()));
                    RpcMessage::response_ok(msgid, result)
                },
                Err(e) => {
                    let err = Value::Map(vec![
                        (
                            Value::String("code".into()),
                            Value::Integer((-32601).into()),
                        ),
                        (
                            Value::String("message".into()),
                            Value::String(e.to_string().into()),
                        ),
                    ]);
                    RpcMessage::response_err(msgid, err)
                },
            }
        },
        RpcMessage::Notify { .. } | RpcMessage::Response { .. } => {
            if matches!(msg, RpcMessage::Response { .. }) {
                warn!("received unexpected Response on admin socket; ignoring");
            }
            RpcMessage::notify("noop", Value::Nil)
        },
    }
}
