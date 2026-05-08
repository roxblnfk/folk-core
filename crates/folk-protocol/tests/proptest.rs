//! Property-based round-trip tests for `RpcMessage` and `FrameCodec`.

use bytes::BytesMut;
use folk_protocol::{FrameCodec, RpcMessage};
use proptest::prelude::*;
use rmpv::Value;
use tokio_util::codec::{Decoder, Encoder};

// --- Strategies for generating arbitrary protocol values ---

fn arb_simple_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Nil),
        any::<bool>().prop_map(Value::Boolean),
        any::<i64>().prop_map(|i| Value::Integer(i.into())),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::F64),
        ".*".prop_map(|s: String| Value::String(s.into())),
        prop::collection::vec(any::<u8>(), 0..=128).prop_map(Value::Binary),
    ]
}

fn arb_rpc_message() -> impl Strategy<Value = RpcMessage> {
    prop_oneof![
        // Request
        (any::<u32>(), "[a-z]{1,20}", arb_simple_value())
            .prop_map(|(msgid, method, params)| RpcMessage::request(msgid, method, params)),
        // Response (ok)
        (any::<u32>(), arb_simple_value())
            .prop_map(|(msgid, result)| RpcMessage::response_ok(msgid, result)),
        // Response (err)
        (any::<u32>(), arb_simple_value())
            .prop_map(|(msgid, err)| RpcMessage::response_err(msgid, err)),
        // Notify
        ("[a-z]{1,20}\\.[a-z]{1,20}", arb_simple_value())
            .prop_map(|(method, params)| RpcMessage::notify(method, params)),
    ]
}

// --- Tests ---

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn rmp_serde_round_trip(msg in arb_rpc_message()) {
        let bytes = rmp_serde::to_vec(&msg).expect("encode");
        let decoded: RpcMessage = rmp_serde::from_slice(&bytes).expect("decode");
        prop_assert_eq!(msg, decoded);
    }

    #[test]
    fn codec_round_trip(msg in arb_rpc_message()) {
        let mut codec = FrameCodec::new();
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).expect("encode");
        let decoded = codec.decode(&mut buf).expect("decode").expect("frame present");

        prop_assert_eq!(msg, decoded);
        prop_assert!(buf.is_empty());
    }

    #[test]
    fn codec_handles_chunked_input(msg in arb_rpc_message()) {
        let mut codec = FrameCodec::new();
        let mut full = BytesMut::new();
        codec.encode(msg.clone(), &mut full).expect("encode");

        // Feed bytes one at a time
        let mut accum = BytesMut::new();
        let mut decoded: Option<RpcMessage> = None;
        for byte in full.iter().copied() {
            accum.extend_from_slice(&[byte]);
            if let Some(m) = codec.decode(&mut accum).expect("decode chunk") {
                decoded = Some(m);
                break;
            }
        }

        prop_assert_eq!(decoded, Some(msg));
    }
}
