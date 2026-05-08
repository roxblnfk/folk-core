//! Error type for the protocol crate.
//!
//! All fallible operations in `folk-protocol` return `Result<T, Error>`.

use std::io;

use thiserror::Error;

/// Errors that can occur in `folk-protocol`.
#[derive(Debug, Error)]
pub enum Error {
    /// The frame's length prefix exceeds the maximum allowed (16 MiB).
    #[error("frame too large: {0} bytes (limit {limit})", limit = MAX_FRAME_SIZE)]
    FrameTooLarge(usize),

    /// The decoded payload is not a valid RPC message (e.g. wrong shape).
    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    /// `MessagePack` decoding failed.
    #[error("decode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    /// `MessagePack` encoding failed.
    #[error("encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    /// Underlying I/O error from the framing layer.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// Maximum allowed frame size, in bytes. Frames larger than this are rejected.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
