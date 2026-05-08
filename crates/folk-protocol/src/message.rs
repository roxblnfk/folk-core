//! `MessagePack`-RPC message types.
//!
//! Three message variants per the `MessagePack`-RPC spec, each serialized as
//! a positional `MessagePack` array:
//!
//! - `Request`: `[0, msgid, method, params]`
//! - `Response`: `[1, msgid, error, result]`
//! - `Notify`: `[2, method, params]`

use rmpv::Value;
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A single RPC message on the wire.
#[derive(Debug, Clone, PartialEq)]
pub enum RpcMessage {
    /// Caller asks for a method invocation; expects a `Response` with matching `msgid`.
    Request {
        msgid: u32,
        method: String,
        params: Value,
    },
    /// Reply to a `Request` with the same `msgid`. Exactly one of `error`/`result`
    /// is meaningful; the other should be `nil` (`Value::Nil`).
    Response {
        msgid: u32,
        error: Value,
        result: Value,
    },
    /// One-way message; no `Response` is expected.
    Notify { method: String, params: Value },
}

impl RpcMessage {
    /// Construct a `Request`.
    pub fn request(msgid: u32, method: impl Into<String>, params: Value) -> Self {
        Self::Request {
            msgid,
            method: method.into(),
            params,
        }
    }

    /// Construct a `Response` carrying a successful result. `error` is set to `Nil`.
    pub fn response_ok(msgid: u32, result: Value) -> Self {
        Self::Response {
            msgid,
            error: Value::Nil,
            result,
        }
    }

    /// Construct a `Response` carrying an error. `result` is set to `Nil`.
    pub fn response_err(msgid: u32, error: Value) -> Self {
        Self::Response {
            msgid,
            error,
            result: Value::Nil,
        }
    }

    /// Construct a `Notify`.
    pub fn notify(method: impl Into<String>, params: Value) -> Self {
        Self::Notify {
            method: method.into(),
            params,
        }
    }
}

impl Serialize for RpcMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Request {
                msgid,
                method,
                params,
            } => {
                let mut seq = serializer.serialize_seq(Some(4))?;
                seq.serialize_element(&0u8)?;
                seq.serialize_element(msgid)?;
                seq.serialize_element(method)?;
                seq.serialize_element(params)?;
                seq.end()
            },
            Self::Response {
                msgid,
                error,
                result,
            } => {
                let mut seq = serializer.serialize_seq(Some(4))?;
                seq.serialize_element(&1u8)?;
                seq.serialize_element(msgid)?;
                seq.serialize_element(error)?;
                seq.serialize_element(result)?;
                seq.end()
            },
            Self::Notify { method, params } => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                seq.serialize_element(&2u8)?;
                seq.serialize_element(method)?;
                seq.serialize_element(params)?;
                seq.end()
            },
        }
    }
}

impl<'de> Deserialize<'de> for RpcMessage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MsgVisitor;

        impl<'de> Visitor<'de> for MsgVisitor {
            type Value = RpcMessage;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a MessagePack-RPC array of 3 or 4 elements")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let kind: u8 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::custom("missing message type tag"))?;

                match kind {
                    0 => {
                        let msgid: u32 = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("request: missing msgid"))?;
                        let method: String = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("request: missing method"))?;
                        let params: Value = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("request: missing params"))?;
                        Ok(RpcMessage::Request {
                            msgid,
                            method,
                            params,
                        })
                    },
                    1 => {
                        let msgid: u32 = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("response: missing msgid"))?;
                        let error: Value = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("response: missing error"))?;
                        let result: Value = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("response: missing result"))?;
                        Ok(RpcMessage::Response {
                            msgid,
                            error,
                            result,
                        })
                    },
                    2 => {
                        let method: String = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("notify: missing method"))?;
                        let params: Value = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::custom("notify: missing params"))?;
                        Ok(RpcMessage::Notify { method, params })
                    },
                    other => Err(de::Error::custom(format!(
                        "unknown message type tag: {other}"
                    ))),
                }
            }
        }

        deserializer.deserialize_seq(MsgVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmpv::Value;

    fn round_trip(msg: &RpcMessage) -> RpcMessage {
        let bytes = rmp_serde::to_vec(msg).expect("encode");
        rmp_serde::from_slice(&bytes).expect("decode")
    }

    #[test]
    fn encodes_request_as_4_element_array_with_tag_0() {
        let msg = RpcMessage::request(7, "echo", Value::String("hi".into()));
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let value: Value = rmp_serde::from_slice(&bytes).unwrap();

        let array = value.as_array().expect("array");
        assert_eq!(array.len(), 4);
        assert_eq!(array[0].as_u64(), Some(0));
        assert_eq!(array[1].as_u64(), Some(7));
        assert_eq!(array[2].as_str(), Some("echo"));
        assert_eq!(array[3].as_str(), Some("hi"));
    }

    #[test]
    fn encodes_response_ok_as_4_element_array_with_tag_1_and_nil_error() {
        let msg = RpcMessage::response_ok(42, Value::Integer(99.into()));
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let value: Value = rmp_serde::from_slice(&bytes).unwrap();

        let array = value.as_array().expect("array");
        assert_eq!(array.len(), 4);
        assert_eq!(array[0].as_u64(), Some(1));
        assert_eq!(array[1].as_u64(), Some(42));
        assert!(array[2].is_nil());
        assert_eq!(array[3].as_u64(), Some(99));
    }

    #[test]
    fn encodes_response_err_as_4_element_array_with_nil_result() {
        let err = Value::String("boom".into());
        let msg = RpcMessage::response_err(42, err);
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let value: Value = rmp_serde::from_slice(&bytes).unwrap();

        let array = value.as_array().expect("array");
        assert_eq!(array.len(), 4);
        assert_eq!(array[0].as_u64(), Some(1));
        assert_eq!(array[2].as_str(), Some("boom"));
        assert!(array[3].is_nil());
    }

    #[test]
    fn encodes_notify_as_3_element_array_with_tag_2() {
        let msg = RpcMessage::notify("control.ready", Value::Map(vec![]));
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let value: Value = rmp_serde::from_slice(&bytes).unwrap();

        let array = value.as_array().expect("array");
        assert_eq!(array.len(), 3);
        assert_eq!(array[0].as_u64(), Some(2));
        assert_eq!(array[1].as_str(), Some("control.ready"));
        assert!(array[2].is_map());
    }

    #[test]
    fn round_trip_request_preserves_all_fields() {
        let msg = RpcMessage::request(
            123,
            "http.handle",
            Value::Array(vec![
                Value::String("GET".into()),
                Value::String("/health".into()),
            ]),
        );
        assert_eq!(msg, round_trip(&msg));
    }

    #[test]
    fn round_trip_response_ok_preserves_result() {
        let msg = RpcMessage::response_ok(7, Value::String("pong".into()));
        assert_eq!(msg, round_trip(&msg));
    }

    #[test]
    fn round_trip_response_err_preserves_error() {
        let msg = RpcMessage::response_err(7, Value::Integer(500.into()));
        assert_eq!(msg, round_trip(&msg));
    }

    #[test]
    fn round_trip_notify_preserves_method_and_params() {
        let msg = RpcMessage::notify("control.heartbeat", Value::Nil);
        assert_eq!(msg, round_trip(&msg));
    }

    #[test]
    fn rejects_unknown_type_tag() {
        let bad = Value::Array(vec![
            Value::Integer(5.into()),
            Value::Integer(0.into()),
            Value::Nil,
            Value::Nil,
        ]);
        let bytes = rmp_serde::to_vec(&bad).unwrap();
        let result: std::result::Result<RpcMessage, _> = rmp_serde::from_slice(&bytes);
        assert!(result.is_err(), "should reject tag != 0/1/2");
    }

    #[test]
    fn rejects_request_with_missing_fields() {
        let bad = Value::Array(vec![Value::Integer(0.into()), Value::Integer(1.into())]);
        let bytes = rmp_serde::to_vec(&bad).unwrap();
        let result: std::result::Result<RpcMessage, _> = rmp_serde::from_slice(&bytes);
        assert!(result.is_err(), "should reject incomplete request");
    }

    #[test]
    fn rejects_non_array_input() {
        let bad = Value::String("not a message".into());
        let bytes = rmp_serde::to_vec(&bad).unwrap();
        let result: std::result::Result<RpcMessage, _> = rmp_serde::from_slice(&bytes);
        assert!(result.is_err(), "should reject non-array input");
    }
}
