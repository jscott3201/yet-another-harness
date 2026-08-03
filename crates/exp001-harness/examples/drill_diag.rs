// Diagnose a failed corruption drill: re-derive the flipped offset from the
// trial seed, locate the entry containing it, and check whether body()'s
// checksum verification rejects that entry.
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use exp001_harness::schema::CommandKind;
use exp001_harness::sidecar::read_confirmed;

fn main() {
    let dir = std::env::args().nth(1).expect("trial dir");
    let dir = std::path::Path::new(&dir);
    let verdict: serde_json::Value =
        serde_json::from_slice(&std::fs::read(std::env::args().nth(2).expect("verdict path")).unwrap()).unwrap();
    let trial_seed = verdict["trial_seed"].as_u64().unwrap();
    let variant = 2u32;

    let confirmed = read_confirmed(&dir.join("sidecar.jsonl")).unwrap();
    let bytes = std::fs::read(dir.join("store/wal.log")).unwrap();

    // Mirror corruption_trial's pick loop exactly.
    let mut rng = ChaCha20Rng::seed_from_u64(trial_seed ^ 0xC0552);
    let (needle, back_off) = loop {
        let rec = &confirmed[rng.random_range(4..confirmed.len() - 2)];
        let payload = &rec.spec.events[0].payload;
        match variant % 3 {
            0 => unreachable!(),
            1 => {
                if rec.spec.kind != CommandKind::ReviewEvidence {
                    break (rec.spec.request_digest.clone().into_bytes(), 0usize);
                }
            }
            _ => {
                let filler = payload.split("\"filler\":\"").nth(1).and_then(|s| s.split('"').next());
                if let Some(f) = filler {
                    if f.len() >= 48 {
                        break (f[..48].as_bytes().to_vec(), payload.len() + 60);
                    }
                }
            }
        }
    };
    let hit = bytes.windows(needle.len()).position(|w| w == needle.as_slice()).expect("needle");
    let target = hit.saturating_sub(back_off);
    println!("needle at {hit}, back_off {back_off}, target {target}, byte now {:#04x}", bytes[target]);

    // Walk entries by header to find which entry spans `target`, then decode
    // each entry's body to see which (if any) fail checksum verification.
    use selene_persist::WalReader;
    let reader = WalReader::open(&dir.join("store/wal.log")).unwrap();
    let stream = reader.iterate(|_| true).unwrap();
    let mut bad = 0u32;
    let mut n = 0u32;
    for entry in stream {
        match entry {
            Ok(view) => {
                n += 1;
                if let Err(e) = view.body() {
                    bad += 1;
                    println!("entry seq={} body() FAILS: {e}", view.header.sequence);
                }
            }
            Err(e) => {
                println!("stream error after {n} entries: {e}");
                break;
            }
        }
    }
    println!("{n} entries walked, {bad} failing body-checksum");
}
