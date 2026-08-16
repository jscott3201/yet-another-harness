use std::{error::Error, fmt, str::FromStr};

use semver::{Op, Version, VersionReq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const MAX_QUALIFIED_ID_BYTES: usize = 128;
const MAX_ID_SEGMENT_BYTES: usize = 63;
const MAX_SERVICE_ID_BYTES: usize = 160;
const MAX_VERSION_BYTES: usize = 128;
const MAX_PACKAGE_PATH_BYTES: usize = 256;

/// A rejected plugin-contract identity or version string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityError {
    kind: &'static str,
    value: String,
    expected: &'static str,
}

impl IdentityError {
    fn new(kind: &'static str, value: impl Into<String>, expected: &'static str) -> Self {
        Self {
            kind,
            value: value.into(),
            expected,
        }
    }

    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub const fn expected(&self) -> &'static str {
        self.expected
    }
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid {} {:?}; expected {}",
            self.kind, self.value, self.expected
        )
    }
}

impl Error for IdentityError {}

macro_rules! qualified_identity {
    ($name:ident, $kind:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
                let value = value.into();
                validate_qualified_name(&value, $kind)?;
                Ok(Self(value))
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

        impl FromStr for $name {
            type Err = IdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

qualified_identity!(
    PluginPackageId,
    "plugin package ID",
    "Canonical lowercase identity of one plugin package."
);
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct VersionedIdentity {
    canonical: String,
    name: String,
    major: u32,
}

impl VersionedIdentity {
    fn new(value: impl Into<String>, kind: &'static str) -> Result<Self, IdentityError> {
        let value = value.into();
        if value.len() > MAX_SERVICE_ID_BYTES {
            return Err(versioned_id_error(kind, value));
        }
        let Some((name, major)) = value.split_once("/v") else {
            return Err(versioned_id_error(kind, value));
        };
        if name.contains('/') || major.is_empty() || major.starts_with('0') {
            return Err(versioned_id_error(kind, value));
        }
        validate_qualified_name(name, kind).map_err(|_| versioned_id_error(kind, value.clone()))?;
        let parsed = major
            .parse::<u32>()
            .map_err(|_| versioned_id_error(kind, value.clone()))?;
        if parsed == 0 || parsed.to_string() != major {
            return Err(versioned_id_error(kind, value));
        }
        let name = name.to_owned();
        Ok(Self {
            canonical: value,
            name,
            major: parsed,
        })
    }
}

macro_rules! versioned_identity {
    ($name:ident, $kind:literal, $docs:literal) => {
        #[doc = $docs]
        ///
        /// The canonical form is `qualified.name/v<major>`, with a positive
        /// `u32` major and no leading zeroes.
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(VersionedIdentity);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
                VersionedIdentity::new(value, $kind).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0.canonical
            }

            pub fn name(&self) -> &str {
                &self.0.name
            }

            pub const fn major(&self) -> u32 {
                self.0.major
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0.canonical)
            }
        }

        impl FromStr for $name {
            type Err = IdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0.canonical)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

versioned_identity!(
    ServiceContractId,
    "service contract ID",
    "A service contract identity with an explicit major version."
);
versioned_identity!(
    CapabilityId,
    "capability ID",
    "A requested host-capability identity with an explicit major version."
);

impl ServiceContractId {
    /// Convert this validated wire identity into the live composition ID.
    pub fn to_compose_id(&self) -> yah_compose::ServiceId {
        yah_compose::ServiceId::new(self.as_str())
    }
}

/// One exact, canonical SemVer package version.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PluginVersion {
    canonical: String,
    parsed: Version,
}

impl PluginVersion {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_VERSION_BYTES {
            return Err(version_error("plugin version", value));
        }
        let parsed =
            Version::parse(&value).map_err(|_| version_error("plugin version", value.clone()))?;
        if parsed.to_string() != value {
            return Err(version_error("plugin version", value));
        }
        Ok(Self {
            canonical: value,
            parsed,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub const fn as_semver(&self) -> &Version {
        &self.parsed
    }
}

impl fmt::Display for PluginVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical)
    }
}

impl FromStr for PluginVersion {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for PluginVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.canonical)
    }
}

impl<'de> Deserialize<'de> for PluginVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// One exact, canonical SemVer version of the semantic SDK.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SdkVersion {
    canonical: String,
    parsed: Version,
}

impl SdkVersion {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_VERSION_BYTES {
            return Err(version_error("SDK version", value));
        }
        let parsed =
            Version::parse(&value).map_err(|_| version_error("SDK version", value.clone()))?;
        if parsed.to_string() != value {
            return Err(version_error("SDK version", value));
        }
        Ok(Self {
            canonical: value,
            parsed,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub const fn as_semver(&self) -> &Version {
        &self.parsed
    }
}

impl fmt::Display for SdkVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical)
    }
}

impl FromStr for SdkVersion {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for SdkVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.canonical)
    }
}

impl<'de> Deserialize<'de> for SdkVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// A non-wildcard, canonical SemVer requirement for the semantic SDK.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SdkVersionRequirement {
    canonical: String,
    parsed: VersionReq,
}

impl SdkVersionRequirement {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_VERSION_BYTES {
            return Err(version_error("SDK version requirement", value));
        }
        let parsed = VersionReq::parse(&value)
            .map_err(|_| version_error("SDK version requirement", value.clone()))?;
        if parsed.comparators.is_empty()
            || parsed
                .comparators
                .iter()
                .any(|comparator| comparator.op == Op::Wildcard)
            || parsed.to_string() != value
        {
            return Err(version_error("SDK version requirement", value));
        }
        Ok(Self {
            canonical: value,
            parsed,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub const fn as_semver(&self) -> &VersionReq {
        &self.parsed
    }

    pub fn matches(&self, version: &SdkVersion) -> bool {
        self.parsed.matches(version.as_semver())
    }
}

impl fmt::Display for SdkVersionRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical)
    }
}

impl FromStr for SdkVersionRequirement {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for SdkVersionRequirement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.canonical)
    }
}

impl<'de> Deserialize<'de> for SdkVersionRequirement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// A canonical path resolved only inside an admitted plugin package.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PackageRelativePath(String);

impl PackageRelativePath {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_PACKAGE_PATH_BYTES
            && !value.starts_with('/')
            && !value.starts_with('-')
            && !value.ends_with('/')
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-')
            })
            && value
                .split('/')
                .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
        if !valid {
            return Err(IdentityError::new(
                "package-relative path",
                value,
                "1..=256 ASCII bytes in nonempty '/'-separated segments using letters, digits, '.', '_', or '-', without an option-like leading '-', absolute paths, '.', or '..' segments",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PackageRelativePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PackageRelativePath {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for PackageRelativePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PackageRelativePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// A caller-supplied package content digest.
///
/// SDK-001 validates the canonical value but does not read or hash a package;
/// package staging and digest verification belong to the supply-chain layer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PackageDigest(String);

impl PackageDigest {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        let Some(hex) = value.strip_prefix("blake3:") else {
            return Err(digest_error(value));
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(digest_error(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PackageDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PackageDigest {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for PackageDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PackageDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

fn validate_qualified_name(value: &str, kind: &'static str) -> Result<(), IdentityError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_QUALIFIED_ID_BYTES
        && value.is_ascii()
        && value.split('.').count() >= 2
        && value.split('.').all(|segment| {
            segment.len() <= MAX_ID_SEGMENT_BYTES && valid_qualified_segment(segment)
        });
    if valid {
        Ok(())
    } else {
        Err(IdentityError::new(
            kind,
            value,
            "a lowercase ASCII dotted name with at least two segments; each segment starts with a letter and contains letters, digits, or single interior hyphens",
        ))
    }
}

fn valid_qualified_segment(segment: &str) -> bool {
    let mut bytes = segment.bytes();
    if !matches!(bytes.next(), Some(b'a'..=b'z')) {
        return false;
    }
    let mut previous_hyphen = false;
    for byte in bytes {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' => previous_hyphen = false,
            b'-' if !previous_hyphen => previous_hyphen = true,
            _ => return false,
        }
    }
    !previous_hyphen
}

fn versioned_id_error(kind: &'static str, value: impl Into<String>) -> IdentityError {
    IdentityError::new(
        kind,
        value,
        "a lowercase qualified name followed by '/v' and a positive u32 major without leading zeroes",
    )
}

fn version_error(kind: &'static str, value: impl Into<String>) -> IdentityError {
    IdentityError::new(
        kind,
        value,
        "canonical SemVer syntax; SDK requirements must be explicit and non-wildcard",
    )
}

fn digest_error(value: impl Into<String>) -> IdentityError {
    IdentityError::new(
        "package digest",
        value,
        "'blake3:' followed by exactly 64 lowercase hexadecimal characters",
    )
}
