use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use yah_plugin_host::{
    CapabilityDefinition, CapabilityId, DriverKind, PackageDigest, PluginManifest, PluginRevision,
};

pub const GREETING_CAPABILITY_ID: &str = "example.local-greeting/v1";
pub const GREETING_SUBJECT: &str = "YAH";

/// Example-only synchronous contract.
///
/// The synchronous call borrows inert text and returns an owned string. The
/// contract deliberately exposes no provider, credential, file, socket,
/// callback, or other authority.
pub trait Greeting: Send + Sync + 'static {
    fn greet(&self, subject: &str) -> String;
}

pub fn greeting_definition() -> CapabilityDefinition<dyn Greeting> {
    CapabilityDefinition::new(
        CapabilityId::new(GREETING_CAPABILITY_ID)
            .expect("the checked-in example capability ID is canonical"),
    )
}

pub struct LocalGreeting {
    salutation: &'static str,
    calls: Arc<AtomicUsize>,
}

impl LocalGreeting {
    pub fn new(salutation: &'static str, calls: Arc<AtomicUsize>) -> Self {
        Self { salutation, calls }
    }
}

impl Greeting for LocalGreeting {
    fn greet(&self, subject: &str) -> String {
        self.calls.fetch_add(1, Ordering::AcqRel);
        format!("{}, {subject}!", self.salutation)
    }
}

pub fn provider(salutation: &'static str) -> (Arc<dyn Greeting>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider: Arc<dyn Greeting> = Arc::new(LocalGreeting::new(salutation, Arc::clone(&calls)));
    (provider, calls)
}

pub fn example_manifest() -> Result<PluginManifest, String> {
    PluginManifest::parse_toml_bytes(include_bytes!("yah-plugin.toml"))
        .map_err(|error| format!("example manifest is invalid: {error}"))
}

/// Construct synthetic, unverified fixture identity for local execution only.
pub fn example_revision(digest_byte: char) -> Result<PluginRevision, String> {
    let manifest = example_manifest()?;
    let digest = PackageDigest::new(format!("blake3:{}", digest_byte.to_string().repeat(64)))
        .map_err(|error| format!("example digest is invalid: {error}"))?;
    Ok(PluginRevision::new(manifest, digest))
}

pub fn validate_manifest(revision: &PluginRevision) -> Result<(), String> {
    let manifest = revision.manifest();
    if manifest.entrypoint().driver() != DriverKind::BuiltinRust
        || !manifest.required_services().is_empty()
        || !manifest.provided_services().is_empty()
        || manifest.requested_capabilities().len() != 1
        || manifest.requested_capabilities()[0].capability().as_str() != GREETING_CAPABILITY_ID
    {
        return Err("checked-in local manifest does not match the example contract".to_owned());
    }
    Ok(())
}
