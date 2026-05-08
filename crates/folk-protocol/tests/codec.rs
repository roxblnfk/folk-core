//! Integration tests for `FrameCodec`.
//!
//! Verify framing-layer behavior independent of any specific message content.

use bytes::{Buf, BytesMut};
use folk_protocol::{FrameCodec, MAX_FRAME_SIZE, RpcMessage};
use rmpv::Value;
use tokio_util::codec::{Decoder, Encoder};

fn sample_msg() -> RpcMessage {
    RpcMessage::request(1, "echo", Value::String("hi".into()))
}

#[test]
fn encoded_frame_starts_with_4_byte_be_length() {
    let mut codec = FrameCodec::new();
    let mut buf = BytesMut::new();

    codec.encode(sample_msg(), &mut buf).unwrap();

    #[allow(clippy::cast_possible_truncation)]
    let payload_len = (buf.len() - 4) as u32;
    let prefix = buf.split_to(4);
    let mut prefix_buf = &prefix[..];
    let read_len = prefix_buf.get_u32(); // big-endian by default
    assert_eq!(read_len, payload_len);
}

#[test]
fn returns_none_when_buffer_lacks_length_prefix() {
    let mut codec = FrameCodec::new();
    let mut buf = BytesMut::from(&[0x00, 0x00][..]); // only 2 bytes of a 4-byte prefix

    let result = codec.decode(&mut buf).unwrap();
    assert!(result.is_none());
    assert_eq!(buf.len(), 2, "partial prefix should remain in buffer");
}

#[test]
fn returns_none_when_buffer_has_prefix_but_partial_payload() {
    let mut codec = FrameCodec::new();
    let mut full = BytesMut::new();
    codec.encode(sample_msg(), &mut full).unwrap();

    // Truncate the payload (keep the prefix + first 2 bytes of payload)
    let mut partial = full.split_to(6);

    let result = codec.decode(&mut partial).unwrap();
    assert!(result.is_none());
}

#[test]
fn decodes_two_back_to_back_frames_in_one_buffer() {
    let mut codec = FrameCodec::new();
    let mut buf = BytesMut::new();

    let m1 = RpcMessage::request(1, "first", Value::Nil);
    let m2 = RpcMessage::notify("control.ready", Value::Nil);

    codec.encode(m1.clone(), &mut buf).unwrap();
    codec.encode(m2.clone(), &mut buf).unwrap();

    let d1 = codec.decode(&mut buf).unwrap().expect("first frame");
    assert_eq!(d1, m1);

    let d2 = codec.decode(&mut buf).unwrap().expect("second frame");
    assert_eq!(d2, m2);

    assert!(buf.is_empty());
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn rejects_frame_with_length_above_max() {
    let mut codec = FrameCodec::new();

    // Construct a buffer that *claims* a length larger than MAX_FRAME_SIZE
    #[allow(clippy::cast_possible_truncation)]
    let bogus_len = (MAX_FRAME_SIZE + 1) as u32;
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&bogus_len.to_be_bytes());

    let result = codec.decode(&mut buf);
    assert!(
        result.is_err(),
        "should reject frame with length > MAX_FRAME_SIZE"
    );
}

#[test]
fn encode_writes_length_prefix_then_payload() {
    let mut codec = FrameCodec::new();
    let mut buf = BytesMut::new();

    codec.encode(sample_msg(), &mut buf).unwrap();

    let claimed_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    assert_eq!(claimed_len as usize, buf.len() - 4);
}

#[test]
fn encoded_frame_is_decodable_in_isolation() {
    let mut codec = FrameCodec::new();
    let mut encoded = BytesMut::new();
    let original = sample_msg();
    codec.encode(original.clone(), &mut encoded).unwrap();

    // Use a fresh codec instance to decode
    let mut codec2 = FrameCodec::new();
    let decoded = codec2.decode(&mut encoded).unwrap().expect("decoded frame");
    assert_eq!(decoded, original);
}
