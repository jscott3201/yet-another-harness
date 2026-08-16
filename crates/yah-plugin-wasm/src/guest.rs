//! Guest components the driver corpus instantiates, carried as component text.
//!
//! These are fixtures, not a guest SDK and not an authoring example. They exist
//! so the driver can be proved against a component that Wasmtime actually
//! compiles and runs, rather than against a Rust fake standing in for one. Text
//! keeps the corpus reviewable in a diff; a checked-in `.wasm` would not be.
//!
//! Building these from a real language toolchain would need a second Rust
//! target in the gate container. That belongs with the guest SDK work, not
//! here.

/// Which fixture component an activation instantiates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GuestProgram {
    /// Activates cleanly and returns one fixed fixture-tool response.
    ///
    /// `invoke` ignores its input; the answer is a constant.
    Conformant,
    /// Activates with a returned `guest-error` rather than a trap.
    ActivateFailure,
}

impl GuestProgram {
    /// Component text compiled once per driver.
    pub const fn text(self) -> &'static str {
        match self {
            Self::Conformant => CONFORMANT,
            Self::ActivateFailure => ACTIVATE_FAILURE,
        }
    }
}

const CONFORMANT: &str = include_str!("../guests/conformant.wat");
const ACTIVATE_FAILURE: &str = include_str!("../guests/activate-failure.wat");
