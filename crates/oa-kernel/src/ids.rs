//! Identifier and fencing vocabulary shared by every kernel domain.
//!
//! ADR-001 §1 fixes the notation: `Uuid7` is the minting convention and
//! internal storage type — never a wire contract (ADR-002 P3.3 owns wire
//! identifiers, and no authority decision may depend on parsing one).

use serde::{Deserialize, Serialize};
use std::fmt;

/// BLAKE3-256 digest in the only wire form the design admits:
/// `blake3:<64 lowercase hex>` (ADR-001 §1, ADR-002 P3.3).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Digest(String);

impl Digest {
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Digest(format!("blake3:{}", blake3::hash(bytes).to_hex()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Digest {
    type Error = String;

    fn try_from(s: String) -> Result<Self, String> {
        let hex = s
            .strip_prefix("blake3:")
            .ok_or_else(|| format!("digest missing blake3: prefix: {s:?}"))?;
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(format!("digest is not 64 lowercase hex bytes: {s:?}"));
        }
        Ok(Digest(s))
    }
}

impl From<Digest> for String {
    fn from(d: Digest) -> String {
        d.0
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Time-ordered UUIDv7, minted from injected milliseconds and entropy so a
/// fixture run is replayable byte-for-byte — the kernel never reads the wall
/// clock on a deterministic path (MILE-001 Component 1 replay rule).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct Uuid7(u128);

impl Uuid7 {
    /// `unix_ms` fills the 48-bit timestamp field; `entropy` fills rand_a and
    /// rand_b. Version and variant bits are forced, so two mints with equal
    /// inputs are equal — determinism is the point, uniqueness is the minting
    /// discipline of the caller (a seeded, non-repeating source).
    pub fn mint(unix_ms: u64, entropy: u128) -> Self {
        let ts = (unix_ms as u128 & 0xFFFF_FFFF_FFFF) << 80;
        let rand_a = (entropy >> 64) & 0x0FFF;
        let rand_b = entropy & 0x3FFF_FFFF_FFFF_FFFF;
        Uuid7(ts | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b)
    }
}

impl fmt::Display for Uuid7 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = self.0.to_be_bytes();
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7],
            b[8],
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15]
        )
    }
}

impl fmt::Debug for Uuid7 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl From<Uuid7> for String {
    fn from(u: Uuid7) -> String {
        u.to_string()
    }
}

impl TryFrom<String> for Uuid7 {
    type Error = String;

    fn try_from(s: String) -> Result<Self, String> {
        let hex: String = s.chars().filter(|c| *c != '-').collect();
        if s.len() != 36 || hex.len() != 32 {
            return Err(format!("not a hyphenated uuid: {s:?}"));
        }
        let v = u128::from_str_radix(&hex, 16).map_err(|e| format!("bad uuid {s:?}: {e}"))?;
        Ok(Uuid7(v))
    }
}

/// Retry axis: counts operator/scheduler retries of a unit. Starts at 1,
/// strictly increasing, never reused (ADR-001 §3.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AttemptEpoch(pub u64);

/// Invalidation axis: server-internal counter bumped to kill outstanding
/// tokens without minting a new attempt — policy change, approval
/// invalidation, budget revocation, plan supersession (ADR-001 §3.4).
/// NOT reset by an attempt-epoch increment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Stamp(pub u64);

/// Per-control-graph daemon ownership epoch, incremented by one in every
/// takeover claim transaction (ADR-001 §7.3). Chubby's sequencer rule at the
/// repository boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AuthorityEpoch(pub u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_wire_form_round_trips() {
        let d = Digest::of_bytes(b"abc");
        let s = d.as_str().to_owned();
        assert!(s.starts_with("blake3:"));
        assert_eq!(Digest::try_from(s).unwrap(), d);
    }

    #[test]
    fn digest_rejects_uppercase_and_wrong_length() {
        assert!(Digest::try_from(format!("blake3:{}", "A".repeat(64))).is_err());
        assert!(Digest::try_from("blake3:abc".to_owned()).is_err());
        assert!(Digest::try_from("sha256:".to_owned() + &"a".repeat(64)).is_err());
    }

    #[test]
    fn uuid7_is_deterministic_and_ordered_by_time() {
        let a = Uuid7::mint(1, 42);
        let b = Uuid7::mint(1, 42);
        let c = Uuid7::mint(2, 0);
        assert_eq!(a, b);
        assert!(c > a);
        // Version and variant nibbles land where RFC 9562 puts them.
        let s = a.to_string();
        assert_eq!(&s[14..15], "7");
        assert!(matches!(&s[19..20], "8" | "9" | "a" | "b"));
        assert_eq!(Uuid7::try_from(s.clone()).unwrap(), a);
    }
}
