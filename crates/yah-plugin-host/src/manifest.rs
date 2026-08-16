use std::{collections::BTreeSet, error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    CapabilityId, PackageRelativePath, PluginPackageId, PluginVersion, SdkVersionRequirement,
    ServiceContractId,
};

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const MAX_MANIFEST_BYTES: usize = 64 * 1024;
/// Canonical package-relative filename of the authored manifest.
pub const PLUGIN_MANIFEST_FILE: &str = "yah-plugin.toml";
const MAX_SERVICE_DECLARATIONS: usize = 256;
const MAX_CAPABILITY_REQUESTS: usize = 256;

/// One of the execution lanes recognized by the runtime-neutral manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DriverKind {
    BuiltinRust,
    WasmComponent,
    NodeProcess,
    PythonProcess,
}

/// A driver-specific entrypoint with no ambient filesystem interpretation.
///
/// Built-ins carry no caller-selected factory. A later host must require
/// out-of-band trusted provenance and match an exact admitted revision to
/// statically registered code; a parsed package ID grants no such authority.
/// Guest lanes name a canonical logical path that a package loader must
/// resolve and contain.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "driver", rename_all = "kebab-case", deny_unknown_fields)]
pub enum PluginEntrypoint {
    BuiltinRust {},
    WasmComponent { path: PackageRelativePath },
    NodeProcess { path: PackageRelativePath },
    PythonProcess { path: PackageRelativePath },
}

impl PluginEntrypoint {
    pub const fn driver(&self) -> DriverKind {
        match self {
            Self::BuiltinRust {} => DriverKind::BuiltinRust,
            Self::WasmComponent { .. } => DriverKind::WasmComponent,
            Self::NodeProcess { .. } => DriverKind::NodeProcess,
            Self::PythonProcess { .. } => DriverKind::PythonProcess,
        }
    }

    pub fn path(&self) -> Option<&PackageRelativePath> {
        match self {
            Self::BuiltinRust {} => None,
            Self::WasmComponent { path }
            | Self::NodeProcess { path }
            | Self::PythonProcess { path } => Some(path),
        }
    }
}

/// A manifest request for one namespaced host capability.
///
/// This is deliberately not a grant. Admission policy may deny the request,
/// and SDK-003 will own the effective grant and its scoped handles.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapabilityRequest(CapabilityId);

impl CapabilityRequest {
    pub const fn new(capability: CapabilityId) -> Self {
        Self(capability)
    }

    pub const fn capability(&self) -> &CapabilityId {
        &self.0
    }
}

/// One validated, declarative plugin package manifest.
///
/// The manifest requests capabilities but carries no grants, package digest,
/// configuration, activation state, or selected providers.
///
/// Required/provided services are compatibility and routing claims, not
/// authorization. Admission must separately authorize sensitive binding,
/// publication, reserved namespaces, and every requested capability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginManifest {
    package_id: PluginPackageId,
    version: PluginVersion,
    sdk: SdkVersionRequirement,
    entrypoint: PluginEntrypoint,
    required_services: Vec<ServiceContractId>,
    provided_services: Vec<ServiceContractId>,
    requested_capabilities: Vec<CapabilityRequest>,
}

impl PluginManifest {
    /// Construct a validated descriptor from already typed values.
    ///
    /// Construction is not package admission. In particular, accepting a
    /// built-in entrypoint requires trusted provenance that this value does
    /// not carry.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        package_id: PluginPackageId,
        version: PluginVersion,
        sdk: SdkVersionRequirement,
        entrypoint: PluginEntrypoint,
        required_services: Vec<ServiceContractId>,
        provided_services: Vec<ServiceContractId>,
        requested_capabilities: Vec<CapabilityRequest>,
    ) -> Result<Self, ManifestError> {
        validate_count(
            DeclarationKind::RequiredService,
            required_services.len(),
            MAX_SERVICE_DECLARATIONS,
        )?;
        validate_count(
            DeclarationKind::ProvidedService,
            provided_services.len(),
            MAX_SERVICE_DECLARATIONS,
        )?;
        validate_count(
            DeclarationKind::CapabilityRequest,
            requested_capabilities.len(),
            MAX_CAPABILITY_REQUESTS,
        )?;
        reject_duplicate(
            DeclarationKind::RequiredService,
            required_services.iter().map(ServiceContractId::as_str),
        )?;
        reject_duplicate(
            DeclarationKind::ProvidedService,
            provided_services.iter().map(ServiceContractId::as_str),
        )?;
        reject_duplicate(
            DeclarationKind::CapabilityRequest,
            requested_capabilities
                .iter()
                .map(|request| request.capability().as_str()),
        )?;

        let manifest = Self {
            package_id,
            version,
            sdk,
            entrypoint,
            required_services,
            provided_services,
            requested_capabilities,
        };
        let serialized = manifest
            .to_toml()
            .map_err(|error| ManifestError::Encode(error.to_string()))?;
        if serialized.len() > MAX_MANIFEST_BYTES {
            return Err(ManifestError::TooLarge {
                actual: serialized.len(),
                maximum: MAX_MANIFEST_BYTES,
            });
        }
        Ok(manifest)
    }

    /// Parse the strict `yah-plugin.toml` schema.
    ///
    /// Every structural layer rejects unknown fields. TOML duplicate keys are
    /// rejected by the parser, while repeated semantic declarations are
    /// rejected after their canonical identities are validated.
    pub fn parse_toml_bytes(input: &[u8]) -> Result<Self, ManifestError> {
        if input.len() > MAX_MANIFEST_BYTES {
            return Err(ManifestError::TooLarge {
                actual: input.len(),
                maximum: MAX_MANIFEST_BYTES,
            });
        }
        let source = std::str::from_utf8(input).map_err(ManifestError::InvalidUtf8)?;
        Self::parse_bounded_toml(source)
    }

    /// Parse a manifest already held as UTF-8 text.
    ///
    /// Package loaders should prefer [`Self::parse_toml_bytes`] so the byte
    /// bound is checked before UTF-8 decoding.
    pub fn parse_toml(source: &str) -> Result<Self, ManifestError> {
        Self::parse_toml_bytes(source.as_bytes())
    }

    fn parse_bounded_toml(source: &str) -> Result<Self, ManifestError> {
        let version: ManifestVersionProbe =
            toml::from_str(source).map_err(|error| ManifestError::Decode(error.to_string()))?;
        if version.manifest_version != MANIFEST_SCHEMA_VERSION {
            return Err(ManifestError::UnsupportedSchemaVersion {
                found: version.manifest_version,
                supported: MANIFEST_SCHEMA_VERSION,
            });
        }
        let raw: RawManifest =
            toml::from_str(source).map_err(|error| ManifestError::Decode(error.to_string()))?;
        Self::new(
            raw.id,
            raw.version,
            raw.sdk,
            raw.entrypoint,
            raw.services.required,
            raw.services.provided,
            raw.capabilities.requested,
        )
    }

    /// Serialize this validated value without claiming canonical TOML bytes.
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(&ManifestWire::from(self))
    }

    pub const fn schema_version(&self) -> u32 {
        MANIFEST_SCHEMA_VERSION
    }

    pub const fn package_id(&self) -> &PluginPackageId {
        &self.package_id
    }

    pub const fn version(&self) -> &PluginVersion {
        &self.version
    }

    pub const fn sdk_requirement(&self) -> &SdkVersionRequirement {
        &self.sdk
    }

    pub const fn entrypoint(&self) -> &PluginEntrypoint {
        &self.entrypoint
    }

    pub fn required_services(&self) -> &[ServiceContractId] {
        &self.required_services
    }

    pub fn provided_services(&self) -> &[ServiceContractId] {
        &self.provided_services
    }

    pub fn requested_capabilities(&self) -> &[CapabilityRequest] {
        &self.requested_capabilities
    }
}

/// Why a manifest could not be parsed or constructed as a validated value.
#[derive(Debug)]
pub enum ManifestError {
    TooLarge {
        actual: usize,
        maximum: usize,
    },
    InvalidUtf8(std::str::Utf8Error),
    Decode(String),
    Encode(String),
    UnsupportedSchemaVersion {
        found: u32,
        supported: u32,
    },
    TooManyDeclarations {
        kind: DeclarationKind,
        actual: usize,
        maximum: usize,
    },
    DuplicateDeclaration {
        kind: DeclarationKind,
        identity: String,
    },
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { actual, maximum } => {
                write!(f, "plugin manifest is {actual} bytes; maximum is {maximum}")
            }
            Self::InvalidUtf8(error) => write!(f, "plugin manifest is not UTF-8: {error}"),
            Self::Decode(error) => write!(f, "invalid plugin manifest: {error}"),
            Self::Encode(error) => write!(f, "plugin manifest cannot be serialized: {error}"),
            Self::UnsupportedSchemaVersion { found, supported } => write!(
                f,
                "unsupported manifest schema version {found}; this host accepts {supported}"
            ),
            Self::TooManyDeclarations {
                kind,
                actual,
                maximum,
            } => write!(
                f,
                "manifest has {actual} {kind} declarations; maximum is {maximum}"
            ),
            Self::DuplicateDeclaration { kind, identity } => {
                write!(f, "duplicate {kind} declaration {identity:?}")
            }
        }
    }
}

impl Error for ManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUtf8(error) => Some(error),
            Self::Decode(_) | Self::Encode(_) => None,
            Self::TooLarge { .. }
            | Self::UnsupportedSchemaVersion { .. }
            | Self::TooManyDeclarations { .. }
            | Self::DuplicateDeclaration { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclarationKind {
    RequiredService,
    ProvidedService,
    CapabilityRequest,
}

impl fmt::Display for DeclarationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RequiredService => "required service",
            Self::ProvidedService => "provided service",
            Self::CapabilityRequest => "capability request",
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawManifest {
    #[serde(rename = "manifest_version")]
    _manifest_version: u32,
    id: PluginPackageId,
    version: PluginVersion,
    sdk: SdkVersionRequirement,
    entrypoint: PluginEntrypoint,
    #[serde(default)]
    services: RawServices,
    #[serde(default)]
    capabilities: RawCapabilities,
}

#[derive(Deserialize)]
struct ManifestVersionProbe {
    manifest_version: u32,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServices {
    #[serde(default)]
    required: Vec<ServiceContractId>,
    #[serde(default)]
    provided: Vec<ServiceContractId>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCapabilities {
    #[serde(default)]
    requested: Vec<CapabilityRequest>,
}

#[derive(Serialize)]
struct ManifestWire<'a> {
    manifest_version: u32,
    id: &'a PluginPackageId,
    version: &'a PluginVersion,
    sdk: &'a SdkVersionRequirement,
    entrypoint: &'a PluginEntrypoint,
    services: ServicesWire<'a>,
    capabilities: CapabilitiesWire<'a>,
}

impl<'a> From<&'a PluginManifest> for ManifestWire<'a> {
    fn from(manifest: &'a PluginManifest) -> Self {
        Self {
            manifest_version: MANIFEST_SCHEMA_VERSION,
            id: &manifest.package_id,
            version: &manifest.version,
            sdk: &manifest.sdk,
            entrypoint: &manifest.entrypoint,
            services: ServicesWire {
                required: &manifest.required_services,
                provided: &manifest.provided_services,
            },
            capabilities: CapabilitiesWire {
                requested: &manifest.requested_capabilities,
            },
        }
    }
}

#[derive(Serialize)]
struct ServicesWire<'a> {
    required: &'a [ServiceContractId],
    provided: &'a [ServiceContractId],
}

#[derive(Serialize)]
struct CapabilitiesWire<'a> {
    requested: &'a [CapabilityRequest],
}

fn validate_count(
    kind: DeclarationKind,
    actual: usize,
    maximum: usize,
) -> Result<(), ManifestError> {
    if actual <= maximum {
        Ok(())
    } else {
        Err(ManifestError::TooManyDeclarations {
            kind,
            actual,
            maximum,
        })
    }
}

fn reject_duplicate<'a>(
    kind: DeclarationKind,
    identities: impl Iterator<Item = &'a str>,
) -> Result<(), ManifestError> {
    let mut seen = BTreeSet::new();
    for identity in identities {
        if !seen.insert(identity) {
            return Err(ManifestError::DuplicateDeclaration {
                kind,
                identity: identity.to_owned(),
            });
        }
    }
    Ok(())
}
