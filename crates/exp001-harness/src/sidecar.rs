//! Worker-side evidence journal (EXP-001 §10).
//!
//! One JSON line per *confirmed* commit, appended only after `Store::apply`
//! returned Ok — and Selene's commit blocks until durable-and-published, so
//! every parseable line is a commit the loss bar holds recovery to. Lines are
//! written with a single unbuffered `write()` each: on SIGKILL the bytes are
//! already in the kernel, so only the final line can be torn, and
//! [`read_confirmed`] tolerates exactly that.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::workload::CommitSpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Confirmed {
    pub spec: CommitSpec,
    pub generation: u64,
    pub durable_at: Option<u64>,
    /// Set when a planned-stale spec was ACCEPTED — a bar-6 violation the
    /// worker records at the moment it happens, because the auditor cannot
    /// reconstruct funnel decisions post-hoc.
    pub stale_accepted: bool,
}

pub struct Sidecar {
    file: Mutex<File>,
}

impl Sidecar {
    pub fn create(path: &Path) -> std::io::Result<Sidecar> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Sidecar { file: Mutex::new(file) })
    }

    pub fn append(&self, record: &Confirmed) -> std::io::Result<()> {
        let mut line = serde_json::to_vec(record).expect("record serializes");
        line.push(b'\n');
        // One write() per line, no BufWriter: SIGKILL must not cost us lines
        // the store already confirmed durable.
        self.file.lock().expect("sidecar lock").write_all(&line)
    }
}

/// Parse a sidecar back. A torn final line (the kill landed mid-append) is
/// expected and skipped; a torn line anywhere else is corruption and reported
/// as an error.
pub fn read_confirmed(path: &Path) -> std::io::Result<Vec<Confirmed>> {
    let reader = BufReader::new(File::open(path)?);
    let mut out = Vec::new();
    let mut torn_at: Option<usize> = None;
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Confirmed>(&line) {
            Ok(rec) => {
                if let Some(t) = torn_at {
                    return Err(std::io::Error::other(format!(
                        "sidecar corrupt: unparseable line {t} followed by valid line {idx}"
                    )));
                }
                out.push(rec);
            }
            Err(_) => torn_at = Some(idx),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{UnitPool, WriterStream};

    fn spec() -> CommitSpec {
        WriterStream::new(1, 0).next_spec(&mut UnitPool::new(2)).unwrap()
    }

    #[test]
    fn round_trip_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sidecar.jsonl");
        let sc = Sidecar::create(&path).unwrap();
        for _ in 0..3 {
            sc.append(&Confirmed {
                spec: spec(),
                generation: 1,
                durable_at: Some(1),
                stale_accepted: false,
            })
            .unwrap();
        }
        drop(sc);
        // Simulate a kill mid-append: torn trailing bytes.
        use std::io::Write as _;
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"{\"spec\":{\"writer\":0,\"st").unwrap();
        drop(f);

        let recs = read_confirmed(&path).unwrap();
        assert_eq!(recs.len(), 3);
    }
}
