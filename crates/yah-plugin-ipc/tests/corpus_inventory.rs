//! The shared fuzz-corpus inventory.
//!
//! Every file under `tests/corpus/` is a seed both for the deterministic
//! regression suite (this file, in the normal stable gate) and for the
//! cargo-fuzz targets under `fuzz/` (which are pointed at the same
//! directory). The filename prefix is the contract: it names the class the
//! current implementation must assign to the bytes. That makes drift
//! impossible to miss — an implementation change that reclassifies a seed
//! fails here until the seed is renamed or the change is reverted — and
//! makes the corpus self-describing for the fuzzer.
//!
//! Prefixes:
//!
//! - `frame-clean-` — decodes to at least one whole frame, ends clean.
//! - `frame-truncated-prefix-` / `frame-truncated-frame-` — ends
//!   mid-stream without a violation.
//! - `frame-poison-empty-` / `frame-poison-too-large-` — terminal refusal.
//! - `json-ok-` — strict JSON admission.
//! - `json-syntax-` / `json-duplicate-` / `json-unsafe-integer-` — the
//!   three strict refusals.

use std::fs;
use std::path::{Path, PathBuf};

use yah_plugin_ipc::frame::{EndOfInput, FrameDecoder, FrameStreamError};
use yah_plugin_ipc::strict::StrictJsonError;

const CORPUS_DIR: &str = "tests/corpus";

/// Seeds stay small and reviewable: no opaque crash artifact stands in for
/// a minimized regression.
const MAX_SEED_BYTES: usize = 64 * 1024;

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(CORPUS_DIR)
}

/// How the framing oracle classified one byte stream.
#[derive(Debug, PartialEq, Eq)]
enum FrameClass {
    Clean(Vec<Vec<u8>>),
    TruncatedPrefix,
    TruncatedFrame,
    PoisonEmpty,
    PoisonTooLarge,
}

fn frame_class(bytes: &[u8]) -> FrameClass {
    let mut decoder = FrameDecoder::new();
    decoder.feed(bytes);
    let mut frames = Vec::new();
    loop {
        match decoder.next_frame() {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => break,
            Err(FrameStreamError::EmptyFrame) => return FrameClass::PoisonEmpty,
            Err(FrameStreamError::FrameTooLarge { .. }) => return FrameClass::PoisonTooLarge,
        }
    }
    match decoder.finish() {
        EndOfInput::Clean => FrameClass::Clean(frames),
        EndOfInput::TruncatedPrefix { .. } => FrameClass::TruncatedPrefix,
        EndOfInput::TruncatedFrame { .. } => FrameClass::TruncatedFrame,
    }
}

fn expected_frame_class(name: &str) -> Option<FrameClass> {
    if name.starts_with("frame-clean-") {
        Some(FrameClass::Clean(Vec::new()))
    } else if name.starts_with("frame-truncated-prefix-") {
        Some(FrameClass::TruncatedPrefix)
    } else if name.starts_with("frame-truncated-frame-") {
        Some(FrameClass::TruncatedFrame)
    } else if name.starts_with("frame-poison-empty-") {
        Some(FrameClass::PoisonEmpty)
    } else if name.starts_with("frame-poison-too-large-") {
        Some(FrameClass::PoisonTooLarge)
    } else {
        None
    }
}

fn json_class(bytes: &[u8]) -> &'static str {
    match yah_plugin_ipc::strict::parse(bytes) {
        Ok(_) => "json-ok-",
        Err(StrictJsonError::Syntax(_)) => "json-syntax-",
        Err(StrictJsonError::DuplicateMember(_)) => "json-duplicate-",
        Err(StrictJsonError::UnsafeInteger(_)) => "json-unsafe-integer-",
    }
}

#[test]
fn every_seed_classifies_exactly_as_its_name_declares() {
    let dir = corpus_dir();
    // The session-trace corpus lives beside the seeds and has its own
    // replay test; the byte-seed inventory sees files only.
    let seed_paths: Vec<std::path::PathBuf> = fs::read_dir(&dir)
        .expect("the corpus directory is checked in")
        .filter_map(|entry| {
            let path = entry.expect("readable entry").path();
            (!path.is_dir()).then_some(path)
        })
        .collect();
    let mut names: Vec<String> = seed_paths
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert!(
        names.len() >= 25,
        "the corpus must not silently shrink: {} seeds present",
        names.len()
    );
    for name in &names {
        let bytes = fs::read(dir.join(name)).expect("seed is readable");
        assert!(
            bytes.len() <= MAX_SEED_BYTES,
            "{name} is {} bytes; keep seeds small and minimized",
            bytes.len()
        );
        if let Some(expected) = expected_frame_class(name) {
            let actual = frame_class(&bytes);
            let matches = match (&expected, &actual) {
                (FrameClass::Clean(_), FrameClass::Clean(delivered)) => !delivered.is_empty(),
                (expected, actual) => expected == actual,
            };
            assert!(
                matches,
                "{name} must classify as its prefix, got {actual:?}"
            );
        } else if name.starts_with("json-") {
            let actual = json_class(&bytes);
            assert!(
                name.starts_with(actual),
                "{name} must classify as its prefix, got {:?}",
                actual.trim_end_matches('-')
            );
        } else {
            panic!("{name} does not carry a known class prefix; rename it or extend the inventory");
        }
    }
}

#[test]
fn no_two_seeds_carry_the_same_bytes() {
    let dir = corpus_dir();
    let mut contents: Vec<(String, Vec<u8>)> = fs::read_dir(&dir)
        .expect("the corpus directory is checked in")
        .filter_map(|entry| {
            let path = entry.expect("readable entry").path();
            if path.is_dir() {
                None
            } else {
                let bytes = fs::read(&path).expect("seed is readable");
                Some((
                    path.file_name().unwrap().to_string_lossy().into_owned(),
                    bytes,
                ))
            }
        })
        .collect();
    contents.sort_by(|a, b| a.1.cmp(&b.1));
    for pair in contents.windows(2) {
        assert_ne!(
            pair[0].1, pair[1].1,
            "{:?} and {:?} are the same bytes under two names",
            pair[0].0, pair[1].0
        );
    }
}
