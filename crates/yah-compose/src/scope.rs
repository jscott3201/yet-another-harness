//! Immutable composition-scope lineage for contextual service visibility.

use std::{error::Error, fmt, sync::Arc};

use crate::ScopeId;

/// One node in the live composition scope tree.
///
/// Providers are visible to their own scope and descendants within one root;
/// separate roots are isolated. [`ScopeId`] and parent IDs are diagnostic
/// labels; the retained opaque node lineage is the authority. Effect ownership
/// remains with [`crate::EffectScope`].
#[derive(Clone, Debug)]
pub struct Scope {
    id: ScopeId,
    parent_id: Option<ScopeId>,
    node: Arc<ScopeNode>,
}

#[derive(Debug)]
struct ScopeNode {
    parent: Option<Arc<ScopeNode>>,
    realm: Arc<()>,
}

impl PartialEq for Scope {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.parent_id == other.parent_id
            && Arc::ptr_eq(&self.node, &other.node)
    }
}
impl Eq for Scope {}

impl Scope {
    pub fn root(id: impl Into<ScopeId>) -> Self {
        Self {
            id: id.into(),
            parent_id: None,
            node: Arc::new(ScopeNode {
                parent: None,
                realm: Arc::new(()),
            }),
        }
    }

    /// Describe a direct child of `parent`.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeError::SelfParent`] when the child reuses its parent's
    /// diagnostic identity. Parentage is immutable, so constructing a child
    /// cannot mutate an existing lineage or create a cycle.
    pub fn child(id: impl Into<ScopeId>, parent: &Scope) -> Result<Self, ScopeError> {
        let id = id.into();
        if id == parent.id {
            return Err(ScopeError::SelfParent { id });
        }
        Ok(Self {
            id,
            parent_id: Some(parent.id.clone()),
            node: Arc::new(ScopeNode {
                parent: Some(Arc::clone(&parent.node)),
                realm: Arc::clone(&parent.node.realm),
            }),
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

    pub(crate) fn is_visible_from(&self, consumer: &Scope) -> bool {
        if !Arc::ptr_eq(&self.node.realm, &consumer.node.realm) {
            return false;
        }
        let mut node = Some(Arc::clone(&consumer.node));
        while let Some(current) = node {
            if Arc::ptr_eq(&current, &self.node) {
                return true;
            }
            node = current.parent.clone();
        }
        false
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
