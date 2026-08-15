//! Structural scope identity without service visibility or effect ownership yet.

use std::{error::Error, fmt};

use crate::ScopeId;

/// One node in the live composition scope tree.
///
/// A scope records only explicit parentage in this slice. A future registry will
/// own uniqueness, ancestry validation, inherited services, and reversible
/// effects; constructing this value alone does not claim those behaviors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope {
    id: ScopeId,
    parent_id: Option<ScopeId>,
}

impl Scope {
    pub fn root(id: impl Into<ScopeId>) -> Self {
        Self {
            id: id.into(),
            parent_id: None,
        }
    }

    /// Describe a direct child of `parent`.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeError::SelfParent`] when the child reuses its parent's
    /// identity. Registry-wide uniqueness and longer-cycle validation remain
    /// the responsibility of the future scope registry.
    pub fn child(id: impl Into<ScopeId>, parent: &Scope) -> Result<Self, ScopeError> {
        let id = id.into();
        if id == parent.id {
            return Err(ScopeError::SelfParent { id });
        }
        Ok(Self {
            id,
            parent_id: Some(parent.id.clone()),
        })
    }

    pub fn id(&self) -> &ScopeId {
        &self.id
    }

    pub fn parent_id(&self) -> Option<&ScopeId> {
        self.parent_id.as_ref()
    }

    pub fn is_root(&self) -> bool {
        self.parent_id.is_none()
    }
}

/// Invalid relationship detected without a scope registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScopeError {
    SelfParent { id: ScopeId },
}

impl fmt::Display for ScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScopeError::SelfParent { id } => {
                write!(f, "scope {id} cannot be its own parent")
            }
        }
    }
}

impl Error for ScopeError {}
