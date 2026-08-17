//! Writes the worker protocol's checked-in artifacts; `--check` verifies
//! them without writing, which is what the gate runs.

use std::path::PathBuf;
use yah_plugin_ipc::generate;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    if std::env::args().nth(1).as_deref() == Some("--check") {
        if let Err(detail) = generate::check_checked_in(&root) {
            eprintln!("{detail}");
            std::process::exit(1);
        }
        return;
    }
    let dir = root.join("generated/worker-protocol");
    std::fs::create_dir_all(&dir).expect("create generated worker-protocol directory");
    std::fs::write(dir.join("worker.schema.json"), generate::worker_schema())
        .expect("write worker JSON Schema");
    std::fs::write(dir.join("host.schema.json"), generate::host_schema())
        .expect("write host JSON Schema");
    std::fs::write(dir.join("protocol.ts"), generate::typescript()).expect("write TypeScript");
}
