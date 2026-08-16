use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use yah_compose::CleanupError;

use super::{
    DriverDeactivationError, DriverFuture, DriverStopPermit, PluginActivationId, PluginDriver,
    PreparedDriverActivation, activation::consume_panic_payload,
};

type DeactivationFuture = DriverFuture<Result<(), DriverDeactivationError>>;

pub(super) struct PreparedControl {
    objects: Mutex<PreparedObjects>,
}

#[derive(Default)]
struct PreparedObjects {
    prepared: Option<Arc<dyn PreparedDriverActivation>>,
    driver: Option<Arc<dyn PluginDriver>>,
}

impl PreparedControl {
    pub(super) fn new(driver: Arc<dyn PluginDriver>) -> Self {
        Self {
            objects: Mutex::new(PreparedObjects {
                prepared: None,
                driver: Some(driver),
            }),
        }
    }

    pub(super) fn install_prepared(&self, prepared: Arc<dyn PreparedDriverActivation>) {
        let mut objects = self.lock();
        assert!(objects.prepared.replace(prepared).is_none());
    }

    pub(super) fn with_driver<T>(
        &self,
        operation: impl FnOnce(&dyn PluginDriver) -> T,
    ) -> Option<T> {
        self.lock().driver.as_deref().map(operation)
    }

    pub(super) fn with_prepared<T>(
        &self,
        operation: impl FnOnce(&dyn PreparedDriverActivation) -> T,
    ) -> Option<T> {
        self.lock().prepared.as_deref().map(operation)
    }

    pub(super) fn dispose(&self) -> Option<String> {
        let objects = std::mem::take(&mut *self.lock());
        dispose_objects(objects)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PreparedObjects> {
        self.objects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for PreparedControl {
    fn drop(&mut self) {
        let objects = std::mem::take(
            self.objects
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let _ = dispose_objects(objects);
    }
}

pub(super) struct DriverDeactivation {
    control: Arc<PreparedControl>,
    state: DeactivationState,
}

enum DeactivationState {
    Prepared(Option<PluginActivationId>),
    Running(DeactivationFuture),
    Complete,
    Transitioning,
}

impl DriverDeactivation {
    pub(super) fn new(control: Arc<PreparedControl>, id: PluginActivationId) -> Self {
        Self {
            control,
            state: DeactivationState::Prepared(Some(id)),
        }
    }

    fn finish(&mut self, mut failures: Vec<String>) -> Poll<Result<(), CleanupError>> {
        if let Some(summary) = self.control.dispose() {
            failures.push(format!(
                "plugin activation control destructor panicked: {summary}"
            ));
        }
        self.state = DeactivationState::Complete;
        Poll::Ready(if failures.is_empty() {
            Ok(())
        } else {
            Err(CleanupError::new(failures.join("; ")))
        })
    }
}

impl Future for DriverDeactivation {
    type Output = Result<(), CleanupError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let state = std::mem::replace(&mut self.state, DeactivationState::Transitioning);
            match state {
                DeactivationState::Prepared(mut id) => {
                    let id = id.take().expect("prepared deactivation owns one identity");
                    match catch_unwind(AssertUnwindSafe(|| {
                        self.control.with_prepared(|prepared| {
                            prepared.deactivate(DriverStopPermit::new(id))
                        })
                    })) {
                        Ok(Some(future)) => self.state = DeactivationState::Running(future),
                        Ok(None) => {
                            return self.finish(vec![
                                "prepared plugin control was already released".to_owned(),
                            ]);
                        }
                        Err(payload) => {
                            return self.finish(vec![format!(
                                "driver panicked while constructing deactivation: {}",
                                consume_panic_payload(payload)
                            )]);
                        }
                    }
                }
                DeactivationState::Running(mut future) => {
                    let polled = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx)));
                    match polled {
                        Ok(Poll::Pending) => {
                            self.state = DeactivationState::Running(future);
                            return Poll::Pending;
                        }
                        Ok(Poll::Ready(result)) => {
                            let mut failures = result
                                .err()
                                .map(|error| vec![error.to_string()])
                                .unwrap_or_default();
                            if let Some(summary) = drop_deactivation_future(future) {
                                failures.push(format!(
                                    "deactivation future destructor panicked: {summary}"
                                ));
                            }
                            return self.finish(failures);
                        }
                        Err(payload) => {
                            let mut failures = vec![format!(
                                "driver panicked while polling deactivation: {}",
                                consume_panic_payload(payload)
                            )];
                            if let Some(summary) = drop_deactivation_future(future) {
                                failures.push(format!(
                                    "deactivation future destructor panicked: {summary}"
                                ));
                            }
                            return self.finish(failures);
                        }
                    }
                }
                DeactivationState::Complete => panic!("completed deactivation was polled again"),
                DeactivationState::Transitioning => {
                    unreachable!("deactivation state transitions are not reentrant")
                }
            }
        }
    }
}

impl Drop for DriverDeactivation {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, DeactivationState::Complete);
        if let DeactivationState::Running(future) = state {
            let _ = drop_deactivation_future(future);
        }
        let _ = self.control.dispose();
    }
}

fn drop_deactivation_future(future: DeactivationFuture) -> Option<String> {
    catch_unwind(AssertUnwindSafe(|| drop(future)))
        .err()
        .map(consume_panic_payload)
}

fn dispose_objects(objects: PreparedObjects) -> Option<String> {
    let mut failures = Vec::new();
    if let Some(prepared) = objects.prepared
        && let Some(summary) = drop_arc(prepared)
    {
        failures.push(format!("prepared activation: {summary}"));
    }
    if let Some(driver) = objects.driver
        && let Some(summary) = drop_arc(driver)
    {
        failures.push(format!("plugin driver: {summary}"));
    }
    (!failures.is_empty()).then(|| failures.join("; "))
}

fn drop_arc<T: ?Sized>(value: Arc<T>) -> Option<String> {
    catch_unwind(AssertUnwindSafe(|| drop(value)))
        .err()
        .map(consume_panic_payload)
}
