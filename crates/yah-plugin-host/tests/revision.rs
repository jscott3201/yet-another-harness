use yah_plugin_host::{PackageDigest, PluginManifest, PluginRevision, PluginRevisionId};

fn manifest() -> PluginManifest {
    PluginManifest::parse_toml(
        r#"manifest_version = 1
id = "acme.echo"
version = "1.2.3"
sdk = "^0.1.0"

[entrypoint]
driver = "wasm-component"
path = "plugin.wasm"
"#,
    )
    .unwrap()
}

#[test]
fn package_digest_is_part_of_exact_revision_identity() {
    let first = PluginRevision::new(
        manifest(),
        PackageDigest::new(format!("blake3:{}", "11".repeat(32))).unwrap(),
    );
    let same = PluginRevision::new(
        manifest(),
        PackageDigest::new(format!("blake3:{}", "11".repeat(32))).unwrap(),
    );
    let republished = PluginRevision::new(
        manifest(),
        PackageDigest::new(format!("blake3:{}", "22".repeat(32))).unwrap(),
    );

    assert_eq!(first.id(), same.id());
    assert_ne!(first.id(), republished.id());
    assert_eq!(first.id().package_id().as_str(), "acme.echo");
    assert_eq!(first.id().version().as_str(), "1.2.3");
    assert!(
        first
            .id()
            .to_string()
            .starts_with("acme.echo@1.2.3#blake3:")
    );
    assert_eq!(first.manifest(), &manifest());

    let structured = toml::to_string(first.id()).unwrap();
    assert!(structured.contains("package_id = \"acme.echo\""));
    assert!(structured.contains("package_digest = \"blake3:"));
    assert_eq!(
        toml::from_str::<PluginRevisionId>(&structured).unwrap(),
        *first.id()
    );
}
