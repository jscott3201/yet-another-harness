use yah_compose::{Scope, ScopeError};

#[test]
fn root_and_child_scopes_preserve_explicit_parentage() {
    let root = Scope::root("root");
    let child = Scope::child("child", &root).unwrap();
    let grandchild = Scope::child("grandchild", &child).unwrap();

    assert!(root.is_root());
    assert_eq!(root.parent_id(), None);
    assert!(!child.is_root());
    assert_eq!(child.parent_id(), Some(root.id()));
    assert_eq!(grandchild.parent_id(), Some(child.id()));
}

#[test]
fn child_scope_rejects_direct_self_parentage() {
    let root = Scope::root("scope");

    assert_eq!(
        Scope::child("scope", &root),
        Err(ScopeError::SelfParent {
            id: root.id().clone(),
        })
    );
}
