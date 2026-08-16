//! Opaque identities for the live composition graph.

use std::fmt;

macro_rules! string_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            /// Wrap an identity chosen by the owning layer.
            ///
            /// This live kernel deliberately imposes no package or wire syntax.
            /// Manifest and application boundaries validate their own identities.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
    };
}

string_id!(ComponentId, "Identity of a component definition.");
string_id!(
    ComponentRevisionId,
    "Identity of one immutable component definition, scope, and configuration revision."
);
string_id!(
    ComponentInstanceId,
    "Identity of one mounted component instance."
);
string_id!(
    ScopeId,
    "Diagnostic label for one process-local composition scope."
);
string_id!(
    ServiceId,
    "Stable semantic identity of one service contract."
);
