use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{PackageDigest, PluginManifest, PluginPackageId, PluginVersion};

/// Immutable, syntactically validated identity of one plugin package revision.
///
/// A later trusted staging boundary is expected to supply and verify the
/// package digest. Constructing or deserializing this value proves neither
/// file identity nor package admission. This identity also does not include a
/// component's configuration, grants, scope, or selected providers and must
/// not be reused as a live composition revision ID.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PluginRevisionId {
    package_id: PluginPackageId,
    version: PluginVersion,
    package_digest: PackageDigest,
}

impl PluginRevisionId {
    pub fn new(
        package_id: PluginPackageId,
        version: PluginVersion,
        package_digest: PackageDigest,
    ) -> Self {
        Self {
            package_id,
            version,
            package_digest,
        }
    }

    pub const fn package_id(&self) -> &PluginPackageId {
        &self.package_id
    }

    pub const fn version(&self) -> &PluginVersion {
        &self.version
    }

    pub const fn package_digest(&self) -> &PackageDigest {
        &self.package_digest
    }
}

impl fmt::Display for PluginRevisionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}#{}",
            self.package_id, self.version, self.package_digest
        )
    }
}

/// A validated manifest paired with its host-supplied package digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginRevision {
    id: PluginRevisionId,
    manifest: PluginManifest,
}

impl PluginRevision {
    pub fn new(manifest: PluginManifest, package_digest: PackageDigest) -> Self {
        let id = PluginRevisionId::new(
            manifest.package_id().clone(),
            manifest.version().clone(),
            package_digest,
        );
        Self { id, manifest }
    }

    pub const fn id(&self) -> &PluginRevisionId {
        &self.id
    }

    pub const fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }
}
