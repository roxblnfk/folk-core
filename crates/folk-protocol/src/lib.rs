//! # `folk-protocol`
//!
//! Wire format for Folk: typed RPC messages and length-prefixed framing.
//!
//! ## What this crate provides
//!
//! - [`RpcMessage`]: the three message variants of `MessagePack`-RPC
//!   (`Request`, `Response`, `Notify`), encoded as positional `MessagePack` arrays.
//! - [`FrameCodec`]: a Tokio codec that combines a 4-byte big-endian length prefix
//!   with `rmp_serde` payload encoding/decoding.
//! - [`Error`]: a single error type for all fallible operations in this crate.
//!
//! ## Wire format
//!
//! Every message on the wire is:
//!
//! ```text
//! +----------------+---------------------------------+
//! |  4-byte length |  MessagePack-encoded array      |
//! |  big-endian    |  (the RpcMessage payload)       |
//! +----------------+---------------------------------+
//! ```
//!
//! Maximum frame size is [`MAX_FRAME_SIZE`] (16 MiB). See
//! `folk-spec/spec/02-protocol.md` for the full specification.
//!
//! ## Example
//!
//! ```rust
//! use bytes::BytesMut;
//! use folk_protocol::{FrameCodec, RpcMessage};
//! use rmpv::Value;
//! use tokio_util::codec::{Decoder, Encoder};
//!
//! let mut codec = FrameCodec::new();
//! let msg = RpcMessage::request(1, "echo", Value::String("hello".into()));
//!
//! let mut buf = BytesMut::new();
//! codec.encode(msg.clone(), &mut buf).unwrap();
//!
//! let decoded = codec.decode(&mut buf).unwrap().unwrap();
//! assert_eq!(msg, decoded);
//! ```

pub mod codec;
pub mod error;
pub mod message;

pub use codec::FrameCodec;
pub use error::{Error, MAX_FRAME_SIZE, Result};
pub use message::RpcMessage;
