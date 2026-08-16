use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

use yah_plugin_host::{
    CapabilityBrokerError, CapabilityHandle, DriverActivationError, DriverDeactivationError,
    DriverFuture, DriverHealthError, DriverKind, DriverPrepareError, DriverStartPermit,
    DriverStopPermit, PluginActivationId, PluginActivationRequest, PluginDriver, PluginHealth,
    PluginRevisionId, PreparedDriverActivation,
};

use crate::contract::{GREETING_SUBJECT, Greeting, greeting_definition};

type ObservationMap = HashMap<PluginActivationId, Arc<ActivationObservation>>;
type CapabilityMap = HashMap<PluginActivationId, CapabilityObservation>;

#[derive(Clone, Copy)]
pub struct ActivationPlan {
    start: StartBehavior,
    deactivate: DeactivateBehavior,
    healthy: bool,
}

impl ActivationPlan {
    pub const fn ready() -> Self {
        Self {
            start: StartBehavior::Ready,
            deactivate: DeactivateBehavior::Ready,
            healthy: true,
        }
    }

    pub const fn pending_start() -> Self {
        Self {
            start: StartBehavior::Pending,
            deactivate: DeactivateBehavior::Ready,
            healthy: true,
        }
    }

    pub const fn start_failure() -> Self {
        Self {
            start: StartBehavior::Failure,
            deactivate: DeactivateBehavior::Ready,
            healthy: true,
        }
    }

    pub const fn deactivation_failure() -> Self {
        Self {
            start: StartBehavior::Ready,
            deactivate: DeactivateBehavior::Failure,
            healthy: true,
        }
    }

    #[cfg(test)]
    pub const fn unhealthy() -> Self {
        Self {
            start: StartBehavior::Ready,
            deactivate: DeactivateBehavior::Ready,
            healthy: false,
        }
    }
}

#[derive(Clone, Copy)]
enum StartBehavior {
    Ready,
    Pending,
    Failure,
}

#[derive(Clone, Copy)]
enum DeactivateBehavior {
    Ready,
    Failure,
}

/// Trusted demo/test instrumentation used by this executable example.
///
/// It retains owned strings and mediated weak capability handles so the demo
/// can prove revocation, but never the driver, prepared activation, broker,
/// registration owner, or provider.
#[derive(Clone)]
pub struct ExampleObserver {
    resources: ResourceObserver,
    capabilities: Arc<Mutex<CapabilityMap>>,
}

impl ExampleObserver {
    pub fn greeting(&self, id: &PluginActivationId) -> Option<String> {
        self.capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .and_then(|state| state.greeting.clone())
    }

    pub fn retained_handle(
        &self,
        id: &PluginActivationId,
    ) -> Option<CapabilityHandle<dyn Greeting>> {
        self.capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .and_then(|state| state.handle.clone())
    }

    pub fn denied(&self, id: &PluginActivationId) -> bool {
        self.capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .is_some_and(|state| state.denied)
    }

    pub fn deactivation_calls(&self, id: &PluginActivationId) -> usize {
        self.resources
            .observation(id)
            .map_or(0, |state| state.deactivation_calls.load(Ordering::Acquire))
    }

    pub fn resource_state(&self, id: &PluginActivationId) -> Result<ResourceState, String> {
        self.resources
            .observation(id)
            .ok_or_else(|| format!("unknown example activation {id}"))?
            .resource_state()
    }

    pub fn resource_observer(&self) -> ResourceObserver {
        self.resources.clone()
    }

    #[cfg(test)]
    pub fn activation_ids(&self) -> Vec<PluginActivationId> {
        self.resources
            .observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }
}

/// Read-only fixture evidence suitable for the conformance probe.
///
/// This view does not retain capability handles or runtime control.
#[derive(Clone)]
pub struct ResourceObserver {
    observations: Arc<Mutex<ObservationMap>>,
}

impl ResourceObserver {
    pub fn resource_state(&self, id: &PluginActivationId) -> Result<ResourceState, String> {
        self.observation(id)
            .ok_or_else(|| format!("unknown example activation {id}"))?
            .resource_state()
    }

    fn observation(&self, id: &PluginActivationId) -> Option<Arc<ActivationObservation>> {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }
}

pub struct LocalCapabilityDriver {
    revision: PluginRevisionId,
    plans: Mutex<VecDeque<ActivationPlan>>,
    observations: Arc<Mutex<ObservationMap>>,
    capabilities: Arc<Mutex<CapabilityMap>>,
}

impl LocalCapabilityDriver {
    pub fn scripted(
        revision: PluginRevisionId,
        plans: impl IntoIterator<Item = ActivationPlan>,
    ) -> (Arc<dyn PluginDriver>, ExampleObserver) {
        let observations = Arc::new(Mutex::new(HashMap::new()));
        let capabilities = Arc::new(Mutex::new(HashMap::new()));
        let observer = ExampleObserver {
            resources: ResourceObserver {
                observations: Arc::clone(&observations),
            },
            capabilities: Arc::clone(&capabilities),
        };
        let driver: Arc<dyn PluginDriver> = Arc::new(Self {
            revision,
            plans: Mutex::new(plans.into_iter().collect()),
            observations,
            capabilities,
        });
        (driver, observer)
    }
}

impl PluginDriver for LocalCapabilityDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::BuiltinRust
    }

    fn revision_id(&self) -> &PluginRevisionId {
        &self.revision
    }

    fn prepare(
        &self,
        request: PluginActivationRequest,
    ) -> Result<Arc<dyn PreparedDriverActivation>, DriverPrepareError> {
        let plan = self
            .plans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .ok_or_else(|| DriverPrepareError::new("local example activation plan exhausted"))?;
        let observation = Arc::new(ActivationObservation::new());
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(request.id().clone(), Arc::clone(&observation));
        self.capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(request.id().clone(), CapabilityObservation::default());
        Ok(Arc::new(LocalPreparedActivation {
            id: request.id().clone(),
            plan,
            observation,
            capabilities: Arc::clone(&self.capabilities),
        }))
    }
}

struct LocalPreparedActivation {
    id: PluginActivationId,
    plan: ActivationPlan,
    observation: Arc<ActivationObservation>,
    capabilities: Arc<Mutex<CapabilityMap>>,
}

impl PreparedDriverActivation for LocalPreparedActivation {
    fn id(&self) -> &PluginActivationId {
        &self.id
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        let id = self.id.clone();
        let plan = self.plan;
        let observation = Arc::clone(&self.observation);
        let capabilities = Arc::clone(&self.capabilities);
        Box::pin(async move {
            if permit.id() != &id {
                return Err(DriverActivationError::failed(
                    "host start permit did not match the prepared activation",
                ));
            }
            observation
                .resource
                .store(ResourceState::Live as u8, Ordering::Release);
            resolve_optional_greeting(&id, permit, &capabilities)?;
            match plan.start {
                StartBehavior::Ready => Ok(()),
                StartBehavior::Pending => {
                    std::future::pending::<Result<(), DriverActivationError>>().await
                }
                StartBehavior::Failure => Err(DriverActivationError::failed(
                    "local example returned start failure",
                )),
            }
        })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        if self.plan.healthy && self.observation.resource_state().ok() == Some(ResourceState::Live)
        {
            Ok(PluginHealth::Healthy)
        } else {
            Err(DriverHealthError::new(
                "local example activation resource is not live",
            ))
        }
    }

    fn deactivate(
        &self,
        permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>> {
        let id = self.id.clone();
        let behavior = self.plan.deactivate;
        let observation = Arc::clone(&self.observation);
        let capabilities = Arc::clone(&self.capabilities);
        Box::pin(async move {
            if permit.id() != &id {
                return Err(DriverDeactivationError::new(
                    "host stop permit did not match the prepared activation",
                ));
            }
            observation
                .deactivation_calls
                .fetch_add(1, Ordering::AcqRel);
            observation
                .resource
                .store(ResourceState::Released as u8, Ordering::Release);
            if let Some(state) = capabilities
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_mut(&id)
            {
                state.handle = None;
            }
            match behavior {
                DeactivateBehavior::Ready => Ok(()),
                DeactivateBehavior::Failure => Err(DriverDeactivationError::new(
                    "local example returned deactivation failure",
                )),
            }
        })
    }
}

fn resolve_optional_greeting(
    id: &PluginActivationId,
    permit: DriverStartPermit,
    capabilities: &Mutex<CapabilityMap>,
) -> Result<(), DriverActivationError> {
    let definition = greeting_definition();
    match permit.context().handle(&definition) {
        Ok(handle) => {
            let greeting = handle
                .try_with(|provider| provider.greet(GREETING_SUBJECT))
                .map_err(|error| DriverActivationError::failed(error.to_string()))?;
            let mut evidence = capabilities
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = evidence.get_mut(id).ok_or_else(|| {
                DriverActivationError::failed("missing local capability evidence")
            })?;
            state.greeting = Some(greeting);
            state.handle = Some(handle);
            Ok(())
        }
        Err(CapabilityBrokerError::NotGranted { .. }) => {
            let mut evidence = capabilities
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = evidence.get_mut(id).ok_or_else(|| {
                DriverActivationError::failed("missing local capability evidence")
            })?;
            state.denied = true;
            Ok(())
        }
        Err(error) => Err(DriverActivationError::failed(error.to_string())),
    }
}

#[derive(Default)]
struct CapabilityObservation {
    greeting: Option<String>,
    handle: Option<CapabilityHandle<dyn Greeting>>,
    denied: bool,
}

struct ActivationObservation {
    resource: AtomicU8,
    deactivation_calls: AtomicUsize,
}

impl ActivationObservation {
    fn new() -> Self {
        Self {
            resource: AtomicU8::new(ResourceState::NotAcquired as u8),
            deactivation_calls: AtomicUsize::new(0),
        }
    }

    fn resource_state(&self) -> Result<ResourceState, String> {
        match self.resource.load(Ordering::Acquire) {
            value if value == ResourceState::NotAcquired as u8 => Ok(ResourceState::NotAcquired),
            value if value == ResourceState::Live as u8 => Ok(ResourceState::Live),
            value if value == ResourceState::Released as u8 => Ok(ResourceState::Released),
            value => Err(format!("invalid local resource state {value}")),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceState {
    NotAcquired = 0,
    Live = 1,
    Released = 2,
}
