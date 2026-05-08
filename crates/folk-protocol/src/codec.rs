//! Tokio codec for length-prefixed `MessagePack`-RPC frames.
//!
//! Wraps `tokio_util::codec::LengthDelimitedCodec` for the framing layer and
//! delegates payload encoding/decoding to `rmp_serde`.

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::error::{Error, MAX_FRAME_SIZE, Result};
use crate::message::RpcMessage;

/// Tokio codec for [`RpcMessage`] over a length-prefixed stream.
///
/// Frame layout: `[4-byte BE length][msgpack payload]`. Maximum frame size
/// is [`MAX_FRAME_SIZE`].
///
/// Use this for both the task channel and the control channel — they share
/// the same wire format.
pub struct FrameCodec {
    inner: LengthDelimitedCodec,
}

impl FrameCodec {
    /// Create a new codec with the standard 4-byte BE length prefix and
    /// 16 MiB max frame size.
    pub fn new() -> Self {
        let inner = LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(4)
            .length_adjustment(0)
            .big_endian()
            .max_frame_length(MAX_FRAME_SIZE)
            .new_codec();
        Self { inner }
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for FrameCodec {
    type Item = RpcMessage;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<RpcMessage>> {
        let frame: Option<BytesMut> = self.inner.decode(src).map_err(Error::from)?;
        match frame {
            Some(payload) => {
                let msg: RpcMessage = rmp_serde::from_slice(&payload)?;
                Ok(Some(msg))
            },
            None => Ok(None),
        }
    }
}

impl Encoder<RpcMessage> for FrameCodec {
    type Error = Error;

    fn encode(&mut self, item: RpcMessage, dst: &mut BytesMut) -> Result<()> {
        let payload: Vec<u8> = rmp_serde::to_vec(&item)?;
        let bytes = Bytes::from(payload);
        self.inner.encode(bytes, dst).map_err(Error::from)?;
        Ok(())
    }
}
