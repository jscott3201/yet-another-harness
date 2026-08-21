//! Shared harness helpers for the fuzz targets.
//!
//! Everything here is deterministic given the fuzz input: the chunking is
//! derived from the input itself, so a reproducing input reproduces the
//! exact partition that found a problem.

// Each target links the helpers it needs; the rest are unused in that
// binary, which is fine for a shared module.
#![allow(dead_code)]

/// Bound the work one input can demand. libFuzzer's `-max_len` is the
/// first bound; this guard keeps a manually run or oversized input honest.
pub const MAX_INPUT_BYTES: usize = 256 * 1024;

/// xorshift64*: one seed word, cheap, deterministic.
pub struct Chunker {
    state: u64,
}

impl Chunker {
    pub fn seeded_from(bytes: &[u8]) -> Self {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        for (i, byte) in bytes.iter().take(8).enumerate() {
            seed ^= u64::from(*byte) << (8 * i);
        }
        Self { state: seed | 1 }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Split `bytes` into chunks whose sizes the PRNG chooses in
    /// `1..=max_chunk`, preserving every byte in order.
    pub fn partition<'a>(&mut self, bytes: &'a [u8], max_chunk: usize) -> Vec<&'a [u8]> {
        let mut chunks = Vec::new();
        let mut rest = bytes;
        while !rest.is_empty() {
            let take = (self.next() % max_chunk as u64) as usize + 1;
            let take = take.min(rest.len());
            chunks.push(&rest[..take]);
            rest = &rest[take..];
        }
        chunks
    }
}

/// How one run over a stream classified it, compared across chunkings.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamClass {
    /// The frames delivered, in order, and the stream ended clean.
    Frames(Vec<Vec<u8>>),
    /// The exact framing refusal.
    Poisoned(yah_plugin_ipc::frame::FrameStreamError),
    /// The end-of-input classification for a stream that never violated.
    Truncated(yah_plugin_ipc::frame::EndOfInput),
}

/// Feed `chunks` through a fresh decoder, draining after each feed, and
/// assert the invariants that must hold no matter what arrived: no
/// oversized or empty delivered frame, and retained state within the
/// documented bound at every observation point.
pub fn decode_all(chunks: &[&[u8]]) -> StreamClass {
    use yah_plugin_ipc::frame::{EndOfInput, FrameDecoder};
    use yah_plugin_ipc::MAX_FRAME_BYTES;

    let mut decoder = FrameDecoder::new();
    let mut frames = Vec::new();
    for chunk in chunks {
        decoder.feed(chunk);
        loop {
            match decoder.next_frame() {
                Ok(Some(frame)) => {
                    assert!(
                        !frame.is_empty() && frame.len() <= MAX_FRAME_BYTES,
                        "delivered a {}-byte frame",
                        frame.len()
                    );
                    frames.push(frame);
                }
                Ok(None) => break,
                Err(error) => return StreamClass::Poisoned(error),
            }
        }
        assert!(
            decoder.buffered_len() <= 4 + MAX_FRAME_BYTES,
            "decoder retained {} bytes, over the documented bound",
            decoder.buffered_len()
        );
    }
    match decoder.finish() {
        EndOfInput::Clean => StreamClass::Frames(frames),
        truncated => StreamClass::Truncated(truncated),
    }
}
