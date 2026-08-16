use yah_plugin_host::{
    CapabilityId, PackageDigest, PackageRelativePath, PluginPackageId, PluginVersion, SdkVersion,
    SdkVersionRequirement, ServiceContractId,
};

#[test]
fn qualified_identities_require_canonical_namespaces() {
    for valid in ["acme.issue-context", "yah.graph", "org2.team9.plugin-v2"] {
        assert!(PluginPackageId::new(valid).is_ok(), "{valid}");
    }

    let too_long = format!("acme.{}", "a".repeat(124));
    let segment_too_long = format!("acme.{}", "a".repeat(64));
    for invalid in [
        "",
        "acme",
        "Acme.plugin",
        "acme.Plugin",
        "1acme.plugin",
        "acme.1plugin",
        ".acme",
        "acme.",
        "acme..plugin",
        "acme._plugin",
        "acme.-plugin",
        "acme.plugin-",
        "acme.plugin--next",
        "åcme.plugin",
        &segment_too_long,
        &too_long,
    ] {
        assert!(PluginPackageId::new(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn service_and_capability_ids_require_an_explicit_major() {
    let service = ServiceContractId::new("yah.context.search/v12").unwrap();
    assert_eq!(service.name(), "yah.context.search");
    assert_eq!(service.major(), 12);
    assert_eq!(service.as_str(), "yah.context.search/v12");
    assert_eq!(service.to_compose_id().as_str(), service.as_str());

    let capability = CapabilityId::new("yah.network.connect/v1").unwrap();
    assert_eq!(capability.name(), "yah.network.connect");
    assert_eq!(capability.major(), 1);

    for invalid in [
        "yah.context",
        "yah.context/v",
        "yah.context/v0",
        "yah.context/v01",
        "yah.context/v-1",
        "yah.context/v4294967296",
        "Yah.context/v1",
        "yah.context/v1/extra",
        "yah.context//v1",
    ] {
        assert!(ServiceContractId::new(invalid).is_err(), "{invalid}");
        assert!(CapabilityId::new(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn exact_versions_and_sdk_requirements_are_canonical_and_distinct() {
    for valid in ["0.1.0", "1.2.3-alpha.1", "2.0.0+linux-arm64"] {
        assert_eq!(PluginVersion::new(valid).unwrap().as_str(), valid);
        assert_eq!(SdkVersion::new(valid).unwrap().as_str(), valid);
    }

    for invalid in ["", "v1.2.3", "1", "1.2", "01.2.3", "1.2.3 "] {
        assert!(PluginVersion::new(invalid).is_err(), "{invalid}");
        assert!(SdkVersion::new(invalid).is_err(), "{invalid}");
    }

    let requirement = SdkVersionRequirement::new(">=0.1.0, <0.2.0").unwrap();
    assert!(requirement.matches(&SdkVersion::new("0.1.9").unwrap()));
    assert!(!requirement.matches(&SdkVersion::new("0.2.0").unwrap()));
    assert_eq!(requirement.as_str(), ">=0.1.0, <0.2.0");

    let prerelease = SdkVersionRequirement::new(">=0.2.0-alpha.1, <0.2.0").unwrap();
    assert!(prerelease.matches(&SdkVersion::new("0.2.0-alpha.2").unwrap()));
    assert!(!prerelease.matches(&SdkVersion::new("0.2.0-alpha.0").unwrap()));

    for invalid in ["", "*", "1.*", "1.2.*", ">=0.1.0,<0.2.0", "1.2.3", "latest"] {
        assert!(SdkVersionRequirement::new(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn package_paths_are_portable_relative_logical_paths() {
    for valid in [
        "plugin.wasm",
        "dist/plugin.mjs",
        "src/plugin.ts",
        "python/main.py",
        "bin/plugin_v2-1",
    ] {
        assert_eq!(PackageRelativePath::new(valid).unwrap().as_str(), valid);
    }

    let too_long = format!("src/{}", "a".repeat(253));
    for invalid in [
        "",
        "/src/plugin.ts",
        "src/plugin.ts/",
        "./src/plugin.ts",
        "src/./plugin.ts",
        "src/../plugin.ts",
        "src//plugin.ts",
        r"src\plugin.ts",
        "C:/plugin.ts",
        "-c",
        "-e",
        "--inspect",
        "src/plugin file.ts",
        "src/插件.ts",
        &too_long,
    ] {
        assert!(PackageRelativePath::new(invalid).is_err(), "{invalid:?}");
    }
}

#[test]
fn package_digests_are_canonical_but_not_computed_here() {
    let valid = format!("blake3:{}", "a1".repeat(32));
    assert_eq!(PackageDigest::new(&valid).unwrap().as_str(), valid);

    for invalid in [
        "",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "blake3:abc",
        "blake3:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "blake3:gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
    ] {
        assert!(PackageDigest::new(invalid).is_err(), "{invalid}");
    }
}
