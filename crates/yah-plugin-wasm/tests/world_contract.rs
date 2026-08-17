use std::collections::BTreeSet;

use wit_parser::{InterfaceId, Resolve, WorldItem, WorldKey};

const WIT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/wit");

/// Every `.wit` file in the canonical directory, with comments stripped.
///
/// Reading the directory rather than one included file keeps a second source
/// from slipping a deferred name past the regression below. Comments are
/// dropped so the contract can explain itself in prose without tripping its own
/// tripwire.
fn declared_source() -> String {
    let mut declared = String::new();
    let mut files = 0usize;
    let entries = std::fs::read_dir(WIT_PATH).expect("canonical WIT directory is readable");
    for entry in entries {
        let path = entry.expect("WIT directory entry is readable").path();
        if path.extension().is_none_or(|extension| extension != "wit") {
            continue;
        }
        files += 1;
        let text = std::fs::read_to_string(&path).expect("WIT source is readable");
        for line in text.lines() {
            declared.push_str(line.split("//").next().unwrap_or_default());
            declared.push('\n');
        }
    }
    assert!(files > 0, "the canonical WIT directory declares no source");
    declared
}

fn interface_identity(resolve: &Resolve, key: &WorldKey, item: &WorldItem) -> String {
    let WorldItem::Interface { id, .. } = item else {
        panic!("the conformance world may contain only interfaces");
    };
    let interface = &resolve.interfaces[*id];
    let package_id = interface
        .package
        .expect("world interfaces belong to a declared package");
    let name = interface
        .name
        .as_deref()
        .expect("world interfaces have declared names");
    let identity = resolve.packages[package_id].name.interface_id(name);
    assert_eq!(
        resolve.name_world_key(key),
        identity,
        "the conformance world may not alias an interface"
    );
    identity
}

fn functions(resolve: &Resolve, id: InterfaceId) -> BTreeSet<&str> {
    resolve.interfaces[id]
        .functions
        .keys()
        .map(String::as_str)
        .collect()
}

#[test]
fn world_has_one_versioned_package_and_exact_directional_interfaces() {
    let mut resolve = Resolve::default();
    let (package_id, _) = resolve
        .push_dir(WIT_PATH)
        .expect("canonical WIT package parses");
    let world_id = resolve
        .select_world(&[package_id], Some(yah_plugin_wasm::WIT_WORLD))
        .expect("canonical world resolves");
    let package = &resolve.packages[package_id];
    let world = &resolve.worlds[world_id];

    assert_eq!(resolve.packages.len(), 1);
    assert_eq!(package.name.to_string(), yah_plugin_wasm::WIT_PACKAGE);
    assert_eq!(
        package
            .interfaces
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "cancellation",
            "capabilities",
            "fixture-tool",
            "lifecycle",
            "logging",
        ])
    );
    assert_eq!(package.worlds.len(), 1);
    assert_eq!(world.name, yah_plugin_wasm::WIT_WORLD);

    let imports = world
        .imports
        .iter()
        .map(|(key, item)| interface_identity(&resolve, key, item))
        .collect::<BTreeSet<_>>();
    let exports = world
        .exports
        .iter()
        .map(|(key, item)| interface_identity(&resolve, key, item))
        .collect::<BTreeSet<_>>();

    assert_eq!(
        imports,
        BTreeSet::from([
            "yah:plugin/cancellation@0.1.0".into(),
            "yah:plugin/capabilities@0.1.0".into(),
            "yah:plugin/logging@0.1.0".into(),
        ])
    );
    assert_eq!(
        exports,
        BTreeSet::from([
            "yah:plugin/fixture-tool@0.1.0".into(),
            "yah:plugin/lifecycle@0.1.0".into(),
        ])
    );
}

#[test]
fn source_declares_no_deferred_interface_or_identifier_names() {
    let declared = declared_source();
    for forbidden in [
        "wasi:",
        "filesystem",
        "network",
        "environment",
        "clock",
        "random",
        "activation-id",
        "grant-id",
        "registration-id",
        "graph",
        "memory",
        "artifact",
    ] {
        assert!(
            !declared.contains(forbidden),
            "canonical source unexpectedly declares `{forbidden}`"
        );
    }
}

#[test]
fn interface_functions_remain_small_and_explicit() {
    let mut resolve = Resolve::default();
    let (package_id, _) = resolve
        .push_dir(WIT_PATH)
        .expect("canonical WIT package parses");
    let package = &resolve.packages[package_id];

    let expected = [
        ("logging", BTreeSet::from(["log"])),
        ("cancellation", BTreeSet::from(["is-cancelled"])),
        // wit-parser keys a resource method by its bracketed method name, so
        // the resource itself appears here only through its one method.
        (
            "capabilities",
            BTreeSet::from(["acquire", "[method]capability.invoke"]),
        ),
        ("lifecycle", BTreeSet::from(["activate"])),
        ("fixture-tool", BTreeSet::from(["invoke"])),
    ];
    for (name, expected_functions) in expected {
        let id = package.interfaces[name];
        assert_eq!(functions(&resolve, id), expected_functions);
    }
}

/// The driver's import allowlist is the world's imports, not a copy that drifted.
///
/// `surface::check_import_surface` refuses anything outside `WORLD_IMPORTS`, and
/// that constant is hand-written because the driver has no WIT parser at
/// runtime - `wit-parser` is a dev-dependency. So the constant is only as good
/// as this test: add an import to the world without adding it there and the
/// driver would refuse every guest that uses it, which is a failure this catches
/// at the contract rather than in the field.
#[test]
fn the_import_allowlist_matches_the_world() {
    let mut resolve = Resolve::default();
    let (package_id, _) = resolve
        .push_dir(WIT_PATH)
        .expect("canonical WIT package parses");
    let world_id = resolve
        .select_world(&[package_id], Some(yah_plugin_wasm::WIT_WORLD))
        .expect("canonical world resolves");

    let declared = resolve.worlds[world_id]
        .imports
        .iter()
        .map(|(key, item)| interface_identity(&resolve, key, item))
        .collect::<BTreeSet<_>>();
    let allowed = yah_plugin_wasm::WORLD_IMPORTS
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        declared, allowed,
        "the world's imports and the driver's allowlist must be the same set"
    );
}

/// The capability error enums' case order is the fixture's digit convention.
///
/// The capability-consumer fixture reports refusal codes as the ASCII digits
/// of these discriminants, so order is contract, not style: a reorder would
/// renumber every digit assertion in the capability corpus while each case
/// name still existed. Frozen in order, not as a set.
#[test]
fn capability_error_case_order_is_frozen() {
    let mut resolve = Resolve::default();
    let (package_id, _) = resolve
        .push_dir(WIT_PATH)
        .expect("canonical WIT package parses");
    let package = &resolve.packages[package_id];
    let interface = &resolve.interfaces[package.interfaces["capabilities"]];
    let expected = [
        (
            "acquire-error-code",
            vec![
                "invalid-id",
                "not-granted",
                "revoked",
                "unavailable",
                "mismatched",
                "handle-limit",
            ],
        ),
        (
            "call-error-code",
            vec!["revoked", "exhausted", "invalid-input", "failed"],
        ),
    ];
    for (name, cases) in expected {
        let type_id = interface.types[name];
        let wit_parser::TypeDefKind::Enum(declared) = &resolve.types[type_id].kind else {
            panic!("{name} must be an enum");
        };
        let declared: Vec<&str> = declared
            .cases
            .iter()
            .map(|case| case.name.as_str())
            .collect();
        assert_eq!(declared, cases, "{name} case order is load-bearing");
    }
}
