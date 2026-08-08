#![allow(dead_code)]

mod protocol;

use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    if std::env::args().nth(1).as_deref() == Some("--check") {
        if let Err(detail) = protocol::generate::check_checked_in(&root) {
            eprintln!("{detail}");
            std::process::exit(1);
        }
        return;
    }

    let dir = root.join("generated/protocol");
    std::fs::create_dir_all(&dir).expect("create generated protocol directory");
    std::fs::write(
        dir.join("client.schema.json"),
        protocol::generate::client_schema(),
    )
    .expect("write client JSON Schema");
    std::fs::write(
        dir.join("server.schema.json"),
        protocol::generate::server_schema(),
    )
    .expect("write server JSON Schema");
    std::fs::write(dir.join("protocol.ts"), protocol::generate::typescript())
        .expect("write TypeScript");
}
