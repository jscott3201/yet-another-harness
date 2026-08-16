use yah_plugin_host::{
    CapabilityId, CapabilityRequest, DeclarationKind, DriverKind, MAX_MANIFEST_BYTES,
    ManifestError, PackageRelativePath, PluginEntrypoint, PluginManifest, PluginPackageId,
    PluginVersion, SdkVersionRequirement, ServiceContractId,
};

fn manifest_source(entrypoint: &str) -> String {
    format!(
        r#"manifest_version = 1
id = "acme.issue-context"
version = "0.1.0"
sdk = ">=0.1.0, <0.2.0"

[entrypoint]
{entrypoint}

[services]
required = ["yah.context/v1", "yah.graph.read/v1"]
provided = ["acme.issue-context/v1"]

[capabilities]
requested = ["yah.network.connect/v1", "yah.secrets.read/v1"]
"#
    )
}

#[test]
fn documented_manifest_fixture_parses_verbatim() {
    let manifest = PluginManifest::parse_toml_bytes(include_bytes!("fixtures/yah-plugin.toml"))
        .expect("the documented manifest is an acceptance fixture");
    assert_eq!(manifest.package_id().as_str(), "acme.issue-context");
    assert_eq!(manifest.entrypoint().driver(), DriverKind::NodeProcess);

    let documentation = include_str!("../../../docs/plugin-manifest.md");
    let documented_toml = documentation
        .split_once("```toml\n")
        .and_then(|(_, rest)| rest.split_once("\n```").map(|(example, _)| example))
        .expect("the public contract contains one TOML example");
    assert_eq!(
        PluginManifest::parse_toml(documented_toml).unwrap(),
        manifest
    );
}

#[test]
fn every_initial_driver_lane_has_one_unambiguous_entrypoint() {
    let cases = [
        ("driver = \"builtin-rust\"", DriverKind::BuiltinRust, None),
        (
            "driver = \"wasm-component\"\npath = \"dist/plugin.wasm\"",
            DriverKind::WasmComponent,
            Some("dist/plugin.wasm"),
        ),
        (
            "driver = \"node-process\"\npath = \"src/plugin.ts\"",
            DriverKind::NodeProcess,
            Some("src/plugin.ts"),
        ),
        (
            "driver = \"python-process\"\npath = \"python/plugin.py\"",
            DriverKind::PythonProcess,
            Some("python/plugin.py"),
        ),
    ];

    for (entrypoint, driver, path) in cases {
        let manifest = PluginManifest::parse_toml(&manifest_source(entrypoint)).unwrap();
        assert_eq!(manifest.schema_version(), 1);
        assert_eq!(manifest.package_id().as_str(), "acme.issue-context");
        assert_eq!(manifest.version().as_str(), "0.1.0");
        assert_eq!(manifest.entrypoint().driver(), driver);
        assert_eq!(
            manifest.entrypoint().path().map(|value| value.as_str()),
            path
        );
        assert_eq!(manifest.required_services().len(), 2);
        assert_eq!(manifest.provided_services().len(), 1);
        assert_eq!(manifest.requested_capabilities().len(), 2);

        let encoded = manifest.to_toml().unwrap();
        assert!(encoded.len() <= MAX_MANIFEST_BYTES);
        assert_eq!(PluginManifest::parse_toml(&encoded).unwrap(), manifest);
    }
}

#[test]
fn strict_wire_rejects_unknown_missing_and_authority_fields() {
    let valid = manifest_source("driver = \"node-process\"\npath = \"src/plugin.ts\"");
    let cases = [
        valid.replace(
            "manifest_version = 1",
            "manifest_version = 1\nunknown = true",
        ),
        valid.replace("id = \"acme.issue-context\"\n", ""),
        valid.replace(
            "path = \"src/plugin.ts\"",
            "path = \"src/plugin.ts\"\nargs = []",
        ),
        valid.replace(
            "provided = [\"acme.issue-context/v1\"]",
            "provided = [\"acme.issue-context/v1\"]\nselected = \"provider-1\"",
        ),
        valid.replace(
            "requested = [\"yah.network.connect/v1\", \"yah.secrets.read/v1\"]",
            "requested = [\"yah.network.connect/v1\"]\ngranted = [\"yah.network.connect/v1\"]",
        ),
        valid.replace(
            "version = \"0.1.0\"",
            "version = \"0.1.0\"\ndigest = \"self-declared\"",
        ),
    ];

    for source in cases {
        assert!(
            matches!(
                PluginManifest::parse_toml(&source),
                Err(ManifestError::Decode(_))
            ),
            "{source}"
        );
    }
}

#[test]
fn schema_version_and_document_size_are_bounded_before_admission() {
    let unsupported = manifest_source("driver = \"node-process\"\npath = \"src/plugin.ts\"")
        .replace("manifest_version = 1", "manifest_version = 2");
    assert!(matches!(
        PluginManifest::parse_toml(&unsupported),
        Err(ManifestError::UnsupportedSchemaVersion {
            found: 2,
            supported: 1
        })
    ));

    let unsupported_with_new_shape = format!("{unsupported}\nfuture_field = true\n");
    assert!(matches!(
        PluginManifest::parse_toml(&unsupported_with_new_shape),
        Err(ManifestError::UnsupportedSchemaVersion { found: 2, .. })
    ));

    let oversized = " ".repeat(MAX_MANIFEST_BYTES + 1);
    assert!(matches!(
        PluginManifest::parse_toml_bytes(oversized.as_bytes()),
        Err(ManifestError::TooLarge { .. })
    ));

    let invalid_utf8 = [0xff, 0xfe];
    assert!(matches!(
        PluginManifest::parse_toml_bytes(&invalid_utf8),
        Err(ManifestError::InvalidUtf8(_))
    ));

    let oversized_invalid_utf8 = vec![0xff; MAX_MANIFEST_BYTES + 1];
    assert!(matches!(
        PluginManifest::parse_toml_bytes(&oversized_invalid_utf8),
        Err(ManifestError::TooLarge { .. })
    ));

    let mut at_limit = manifest_source("driver = \"node-process\"\npath = \"src/plugin.ts\"");
    at_limit.push_str(&" ".repeat(MAX_MANIFEST_BYTES - at_limit.len()));
    assert_eq!(at_limit.len(), MAX_MANIFEST_BYTES);
    assert!(PluginManifest::parse_toml_bytes(at_limit.as_bytes()).is_ok());
}

#[test]
fn duplicate_keys_and_semantic_declarations_fail_closed() {
    let base = manifest_source("driver = \"node-process\"\npath = \"src/plugin.ts\"");
    let duplicate_key = base.replace(
        "version = \"0.1.0\"",
        "version = \"0.1.0\"\nversion = \"0.2.0\"",
    );
    assert!(matches!(
        PluginManifest::parse_toml(&duplicate_key),
        Err(ManifestError::Decode(_))
    ));

    for (source, expected_kind) in [
        (
            base.replace(
                "required = [\"yah.context/v1\", \"yah.graph.read/v1\"]",
                "required = [\"yah.context/v1\", \"yah.context/v1\"]",
            ),
            DeclarationKind::RequiredService,
        ),
        (
            base.replace(
                "provided = [\"acme.issue-context/v1\"]",
                "provided = [\"acme.issue-context/v1\", \"acme.issue-context/v1\"]",
            ),
            DeclarationKind::ProvidedService,
        ),
        (
            base.replace(
                "requested = [\"yah.network.connect/v1\", \"yah.secrets.read/v1\"]",
                "requested = [\"yah.network.connect/v1\", \"yah.network.connect/v1\"]",
            ),
            DeclarationKind::CapabilityRequest,
        ),
    ] {
        assert!(matches!(
            PluginManifest::parse_toml(&source),
            Err(ManifestError::DuplicateDeclaration { kind, .. }) if kind == expected_kind
        ));
    }
}

#[test]
fn invalid_identity_version_and_entrypoint_values_are_rejected_during_decode() {
    let base = manifest_source("driver = \"node-process\"\npath = \"src/plugin.ts\"");
    let cases = [
        base.replace("acme.issue-context", "Acme.issue-context"),
        base.replace("version = \"0.1.0\"", "version = \"v0.1.0\""),
        base.replace("sdk = \">=0.1.0, <0.2.0\"", "sdk = \"*\""),
        base.replace("yah.context/v1", "yah.context/v01"),
        base.replace("yah.network.connect/v1", "yah.network.connect"),
        base.replace("path = \"src/plugin.ts\"", "path = \"../plugin.ts\""),
        base.replace("driver = \"node-process\"", "driver = \"native-dylib\""),
        base.replace(
            "path = \"src/plugin.ts\"",
            "factory = \"yah.builtin.plugin\"",
        ),
    ];

    for source in cases {
        assert!(
            matches!(
                PluginManifest::parse_toml(&source),
                Err(ManifestError::Decode(_))
            ),
            "{source}"
        );
    }

    let builtin_with_path = manifest_source("driver = \"builtin-rust\"\npath = \"src/plugin.ts\"");
    assert!(matches!(
        PluginManifest::parse_toml(&builtin_with_path),
        Err(ManifestError::Decode(_))
    ));
}

#[test]
fn programmatic_construction_preserves_the_wire_round_trip_invariant() {
    let manifest = PluginManifest::new(
        PluginPackageId::new("acme.programmatic").unwrap(),
        PluginVersion::new("1.0.0-beta.1+test").unwrap(),
        SdkVersionRequirement::new("^0.1.0").unwrap(),
        PluginEntrypoint::WasmComponent {
            path: PackageRelativePath::new("plugin.wasm").unwrap(),
        },
        vec![ServiceContractId::new("yah.graph.read/v1").unwrap()],
        vec![ServiceContractId::new("acme.programmatic/v1").unwrap()],
        vec![CapabilityRequest::new(
            CapabilityId::new("yah.artifacts.read/v1").unwrap(),
        )],
    )
    .unwrap();

    let encoded = manifest.to_toml().unwrap();
    assert!(encoded.len() <= MAX_MANIFEST_BYTES);
    assert_eq!(PluginManifest::parse_toml(&encoded).unwrap(), manifest);
}

#[test]
fn declaration_counts_and_normalized_document_size_are_bounded() {
    let required = (0..256)
        .map(|index| ServiceContractId::new(format!("acme.required{index}/v1")).unwrap())
        .collect::<Vec<_>>();
    let provided = (0..256)
        .map(|index| ServiceContractId::new(format!("acme.provided{index}/v1")).unwrap())
        .collect::<Vec<_>>();
    let capabilities = (0..256)
        .map(|index| {
            CapabilityRequest::new(CapabilityId::new(format!("acme.capability{index}/v1")).unwrap())
        })
        .collect::<Vec<_>>();
    let at_limit = construct_manifest(required.clone(), provided.clone(), capabilities.clone())
        .expect("short declarations at the count limit fit the document limit");
    assert_eq!(
        PluginManifest::parse_toml(&at_limit.to_toml().unwrap()).unwrap(),
        at_limit
    );

    let extra_service = ServiceContractId::new("acme.extra/v1").unwrap();
    let extra_capability = CapabilityRequest::new(CapabilityId::new("acme.extra/v1").unwrap());
    for (result, kind) in [
        (
            construct_manifest(
                [required.clone(), vec![extra_service.clone()]].concat(),
                vec![],
                vec![],
            ),
            DeclarationKind::RequiredService,
        ),
        (
            construct_manifest(vec![], [provided, vec![extra_service]].concat(), vec![]),
            DeclarationKind::ProvidedService,
        ),
        (
            construct_manifest(
                vec![],
                vec![],
                [capabilities, vec![extra_capability]].concat(),
            ),
            DeclarationKind::CapabilityRequest,
        ),
    ] {
        assert!(matches!(
            result,
            Err(ManifestError::TooManyDeclarations { kind: actual, actual: 257, .. })
                if actual == kind
        ));
    }

    let long_required = (0..256)
        .map(|index| long_service("required", index))
        .collect();
    let long_provided = (0..256)
        .map(|index| long_service("provided", index))
        .collect();
    let long_capabilities = (0..256)
        .map(|index| {
            CapabilityRequest::new(CapabilityId::new(long_contract("capability", index)).unwrap())
        })
        .collect();
    assert!(matches!(
        construct_manifest(long_required, long_provided, long_capabilities),
        Err(ManifestError::TooLarge { .. })
    ));
}

#[test]
fn service_declarations_allow_an_explicit_proxy_shape() {
    let service = ServiceContractId::new("acme.decorated/v1").unwrap();
    let manifest = construct_manifest(vec![service.clone()], vec![service], vec![]).unwrap();
    assert_eq!(manifest.required_services(), manifest.provided_services());
}

#[test]
fn builtin_entrypoints_carry_no_caller_selected_factory() {
    let entrypoint = PluginEntrypoint::BuiltinRust {};
    assert_eq!(entrypoint.driver(), DriverKind::BuiltinRust);
    assert!(entrypoint.path().is_none());

    let with_factory =
        manifest_source("driver = \"builtin-rust\"\nfactory = \"yah.builtin.issue-context\"");
    assert!(matches!(
        PluginManifest::parse_toml(&with_factory),
        Err(ManifestError::Decode(_))
    ));
}

fn construct_manifest(
    required: Vec<ServiceContractId>,
    provided: Vec<ServiceContractId>,
    capabilities: Vec<CapabilityRequest>,
) -> Result<PluginManifest, ManifestError> {
    PluginManifest::new(
        PluginPackageId::new("acme.programmatic").unwrap(),
        PluginVersion::new("1.0.0").unwrap(),
        SdkVersionRequirement::new("^0.1.0").unwrap(),
        PluginEntrypoint::NodeProcess {
            path: PackageRelativePath::new("src/plugin.ts").unwrap(),
        },
        required,
        provided,
        capabilities,
    )
}

fn long_service(kind: &str, index: usize) -> ServiceContractId {
    ServiceContractId::new(long_contract(kind, index)).unwrap()
}

fn long_contract(kind: &str, index: usize) -> String {
    format!(
        "acme.{}.{}-{kind}-{index}/v1",
        "a".repeat(63),
        "b".repeat(42)
    )
}
