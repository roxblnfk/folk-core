//! Phase 65: worker response contract — `collect_stream` semantics.
//!
//! Verifies that `Executor::execute_value` (used by gRPC/jobs) returns a
//! [`ResponseChunk::Return`] value **verbatim** (not coerced into the HTTP
//! `{status, headers, body}` shape) and propagates [`ResponseChunk::Error`]
//! as an `Err`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use folk_api::{Executor, ResponseChunk, WorkerError};
use folk_core::config::WorkersConfig;
use folk_core::runtime::{Runtime, WorkerHandle};
use folk_core::worker_pool::WorkerPool;
use serde_json::json;
use tokio::sync::mpsc;

/// What the worker should emit for each request.
#[derive(Clone)]
enum Emit {
    Return(serde_json::Value),
    Error,
}

struct ContractRuntime {
    emit: Emit,
}

#[async_trait]
impl Runtime for ContractRuntime {
    async fn spawn(&self) -> Result<Box<dyn WorkerHandle>> {
        Ok(Box::new(ContractWorker {
            emit: self.emit.clone(),
        }))
    }
}

struct ContractWorker {
    emit: Emit,
}

#[async_trait]
impl WorkerHandle for ContractWorker {
    fn id(&self) -> u32 {
        1
    }

    async fn ready(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(
        &mut self,
        _method: &str,
        _payload: serde_json::Value,
        _request_id: Arc<str>,
    ) -> Result<serde_json::Value> {
        anyhow::bail!("unused")
    }

    async fn terminate(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute_streaming(
        &mut self,
        _method: &str,
        _payload: serde_json::Value,
        _request_id: Arc<str>,
        stream_tx: mpsc::Sender<ResponseChunk>,
        _body_rx: Option<mpsc::Receiver<bytes::Bytes>>,
    ) -> Result<()> {
        match &self.emit {
            Emit::Return(v) => {
                stream_tx.send(ResponseChunk::Return(v.clone())).await.ok();
            },
            Emit::Error => {
                stream_tx
                    .send(ResponseChunk::Error(WorkerError {
                        message: "boom".to_string(),
                        exception_class: Some("RuntimeException".to_string()),
                        stacktrace: Some("#0 trace".to_string()),
                    }))
                    .await
                    .ok();
            },
        }
        stream_tx.send(ResponseChunk::End).await.ok();
        Ok(())
    }
}

async fn pool_with(emit: Emit) -> Arc<WorkerPool> {
    let runtime = Arc::new(ContractRuntime { emit });
    let config = WorkersConfig {
        count: 1,
        boot_timeout: Duration::from_secs(5),
        ..WorkersConfig::default()
    };
    let pool = WorkerPool::new(runtime, config).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    pool
}

#[tokio::test]
async fn return_value_is_passed_through_verbatim() {
    // A non-HTTP-shaped value (gRPC `{__result}`) must survive intact.
    let value = json!({"__result": "base64-protobuf", "extra": [1, 2, 3]});
    let pool = pool_with(Emit::Return(value.clone())).await;

    let got = pool.execute_value("grpc.call", json!({})).await.unwrap();
    assert_eq!(
        got, value,
        "Return value must not be coerced to {{status,headers,body}}"
    );
}

#[tokio::test]
async fn error_chunk_propagates_as_err() {
    let pool = pool_with(Emit::Error).await;

    let result = pool.execute_value("grpc.call", json!({})).await;
    assert!(result.is_err(), "Error chunk must surface as Err");
    let msg = format!("{:#}", result.unwrap_err());
    assert!(
        msg.contains("boom"),
        "error should carry the message, got: {msg}"
    );
}
