//! What a guest component is allowed to *declare*, checked before it can run.
//!
//! Wasmtime enforces the import surface only as far as it needs to in order to
//! instantiate: an import the linker cannot satisfy is refused, and an import it
//! does not have to satisfy is ignored. Those are not the same rule. An
//! undeclared instance import with no functions in it satisfies itself - there
//! is nothing for the linker to look up - so a component may name
//! `wasi:cli/environment@0.2.0`, compile, instantiate, activate, and answer a
//! tool call with the host none the wiser. Measured, not inferred: that exact
//! component ran to `{"activated":true}` through this driver before this module
//! existed.
//!
//! Nothing escapes through it. An empty instance carries no functions, so it
//! transfers no authority, and the guest gains nothing it did not already have.
//! What it costs is the claim: "the world is the contract" has to mean the whole
//! declared surface, not the subset the linker happens to consult. A component
//! that declares a dependency on WASI is telling the host something true about
//! itself, and a host that cannot see it cannot refuse it - now, or later when a
//! linker grows an interface that makes the declaration load-bearing.
//!
//! So the driver reads the surface itself, at build time, before any store
//! exists. This is the loader-shaped check: it costs one pass over a type table
//! and it fails inert.
//!
//! What it is, exactly, because the difference matters to anyone relying on it:
//! an **exact-name, root-level** check. Three things it does not do, each
//! disclosed in `docs/wasm-plugin-contract.md` rather than fixed here:
//!
//! - It reads the root component's imports only. An undeclared import inside a
//!   *nested* component is invisible to `component_type().imports()`, and such
//!   an artifact still builds and runs.
//! - It matches names by exact string, where Wasmtime's linker matches
//!   semver-compatibly. Today every guest targets `@0.1.0` and the two agree;
//!   when the world versions they will not, and this will refuse a guest the
//!   linker would have accepted. That is the safer direction to be wrong in,
//!   but it is still wrong, and it is the reason a world-evolution rule is owed.
//! - It checks names, not item kinds. An allowlisted name imported as something
//!   other than the world's instance is not caught here; the linker still
//!   refuses it at instantiation if it carries a function, which is why this
//!   narrows the gap rather than closing it.

use wasmtime::{Engine, component::Component};

/// Every import the `yah:plugin@0.1.0` conformance world declares.
///
/// Frozen here and cross-checked against the WIT source by
/// `world_contract::the_import_allowlist_matches_the_world`, so a world that
/// grows an import fails that test rather than silently widening this one.
pub const WORLD_IMPORTS: &[&str] = &["yah:plugin/logging@0.1.0", "yah:plugin/cancellation@0.1.0"];

/// Refuse a component that declares an import the world does not.
///
/// Named rather than counted: an operator reading the refusal needs to know
/// which interface the guest asked for, because that is the whole content of the
/// finding. The first offender is enough to refuse on, and listing every one
/// would put guest-controlled text into the host's error in unbounded quantity.
pub fn check_import_surface(engine: &Engine, component: &Component) -> Result<(), String> {
    for (name, _item) in component.component_type().imports(engine) {
        if !WORLD_IMPORTS.contains(&name) {
            return Err(format!(
                "component imports `{name}`, which the `{}` world does not declare",
                crate::WIT_WORLD
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::WasmLimits;

    /// The allowlist is the world's imports, not a superset that happens to work.
    #[test]
    fn the_allowlist_names_both_world_imports_and_nothing_else() {
        assert_eq!(
            WORLD_IMPORTS,
            &["yah:plugin/logging@0.1.0", "yah:plugin/cancellation@0.1.0"]
        );
    }

    /// A component importing nothing has nothing to refuse.
    #[test]
    fn a_component_that_imports_nothing_passes() {
        let engine = WasmLimits::default().engine().expect("engine builds");
        let component = Component::new(&engine, "(component)").expect("empty component compiles");
        assert_eq!(check_import_surface(&engine, &component), Ok(()));
    }

    /// The case Wasmtime does not refuse on its own, and the name it reports.
    ///
    /// Two components with different undeclared names, because one alone cannot
    /// tell a refusal that read the import from one that emits a constant - and
    /// the name is the whole content of the finding.
    #[test]
    fn an_empty_undeclared_instance_import_is_refused_by_name() {
        let engine = WasmLimits::default().engine().expect("engine builds");
        for (text, expected) in [
            (
                r#"(component
                     (type $empty (instance))
                     (import "wasi:cli/environment@0.2.0" (instance (type $empty))))"#,
                "wasi:cli/environment@0.2.0",
            ),
            (
                r#"(component (import "acme:other/thing@1.0.0" (func)))"#,
                "acme:other/thing@1.0.0",
            ),
        ] {
            let component = Component::new(&engine, text)
                .expect("an undeclared import still compiles - that is the point");
            let refused = check_import_surface(&engine, &component)
                .expect_err("an import outside the world must be refused");
            assert!(
                refused.contains(expected),
                "the refusal must name what the guest asked for: expected {expected:?} in {refused:?}"
            );
        }
    }

    /// A world import is not refused by the rule that refuses the others.
    #[test]
    fn a_declared_world_import_passes() {
        let engine = WasmLimits::default().engine().expect("engine builds");
        let component = Component::new(
            &engine,
            r#"(component
                 (type $empty (instance))
                 (import "yah:plugin/logging@0.1.0" (instance (type $empty))))"#,
        )
        .expect("a world import compiles");
        assert_eq!(check_import_surface(&engine, &component), Ok(()));
    }
}
