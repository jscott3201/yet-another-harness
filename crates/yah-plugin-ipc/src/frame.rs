//! Length-prefixed framing: a 4-byte big-endian byte count, then exactly
//! that many bytes of one strict-JSON frame.
//!
//! The prefix is the whole point. A delimiter scan and a header grammar
//! both read unbounded bytes before they know the bound; a length prefix
//! lets [`FrameDecoder`] refuse an oversize frame after four bytes, before
//! one payload byte is buffered. The decoder is incremental and sans-io:
//! feed it whatever chunks the transport produced — a frame split across
//! reads is ordinary, not an error — and drain complete frames out.
//!
//! End-of-input is classified, not ignored: a transport that closes mid
//! prefix or mid payload is a truncated frame, which the session settles as
//! outcome-unknown. A decoder panic or a hang on half a frame is exactly
//! the failure mode this module exists to make unrepresentable.

use crate::MAX_FRAME_BYTES;

/// Prefix a serialized frame for the wire.
///
/// Panics if `frame` exceeds [`MAX_FRAME_BYTES`] or `u32::MAX`: encoding an
/// oversize frame is a bug on the sending side, not a wire condition.
pub fn encode(frame: &[u8]) -> Vec<u8> {
    assert!(
        frame.len() <= MAX_FRAME_BYTES,
        "refusing to encode a {}-byte frame over the {}-byte bound",
        frame.len(),
        MAX_FRAME_BYTES
    );
    let mut wire = Vec::with_capacity(4 + frame.len());
    wire.extend_from_slice(
        &u32::try_from(frame.len())
            .expect("bounded above")
            .to_be_bytes(),
    );
    wire.extend_from_slice(frame);
    wire
}

/// Why the byte stream stopped being a frame stream. Both cases poison the
/// decoder: framing carries no resync marker, so after one violation every
/// later byte is unattributable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameStreamError {
    /// A prefix declared more than [`MAX_FRAME_BYTES`]. Refused before any
    /// payload byte was buffered.
    FrameTooLarge { declared: u64 },
    /// A prefix declared zero bytes; an empty frame is not a value.
    EmptyFrame,
}

/// How the input ended, from [`FrameDecoder::finish`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndOfInput {
    /// The last byte was the last byte of a frame.
    Clean,
    /// Input ended inside a length prefix.
    TruncatedPrefix { have: usize },
    /// Input ended inside a frame body.
    TruncatedFrame { declared: usize, have: usize },
}

/// Incremental decoder for one direction of the wire.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
    poisoned: Option<FrameStreamError>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffer transport bytes. Bounded: between [`Self::next`] drains the
    /// buffer never holds more than one maximal frame plus one prefix.
    pub fn feed(&mut self, bytes: &[u8]) {
        if self.poisoned.is_none() {
            self.buffer.extend_from_slice(bytes);
        }
    }

    /// Drain the next complete frame, if one is buffered.
    ///
    /// `Err` reports the stream violation and every later call repeats it;
    /// the connection has no valid continuation.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameStreamError> {
        if let Some(error) = &self.poisoned {
            return Err(error.clone());
        }
        if self.buffer.len() < 4 {
            return Ok(None);
        }
        let declared = u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;
        if declared == 0 {
            return Err(self.poison(FrameStreamError::EmptyFrame));
        }
        if declared > MAX_FRAME_BYTES {
            return Err(self.poison(FrameStreamError::FrameTooLarge {
                declared: declared as u64,
            }));
        }
        if self.buffer.len() < 4 + declared {
            return Ok(None);
        }
        let frame = self.buffer[4..4 + declared].to_vec();
        self.buffer.drain(..4 + declared);
        Ok(Some(frame))
    }

    /// Classify end-of-input against whatever is still buffered. Call after
    /// draining [`Self::next`] to exhaustion.
    pub fn finish(&self) -> EndOfInput {
        if self.buffer.is_empty() {
            return EndOfInput::Clean;
        }
        if self.buffer.len() < 4 {
            return EndOfInput::TruncatedPrefix {
                have: self.buffer.len(),
            };
        }
        let declared = u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;
        EndOfInput::TruncatedFrame {
            declared,
            have: self.buffer.len() - 4,
        }
    }

    fn poison(&mut self, error: FrameStreamError) -> FrameStreamError {
        self.buffer.clear();
        self.poisoned = Some(error.clone());
        error
    }
}
