use super::{
    DriverConformanceCase, DriverConformanceCaseResult, DriverConformanceReport,
    DriverConformanceTarget, cases,
};

/// Run one independently selectable semantic case.
///
/// The returned future is executor-neutral. The harness uses manual polling
/// for deterministic pending operations and supplies no timeout; the caller's
/// test runner remains responsible for an outer execution bound.
pub async fn run_driver_conformance_case(
    target: &dyn DriverConformanceTarget,
    case: DriverConformanceCase,
) -> DriverConformanceCaseResult {
    match cases::run(target, case).await {
        Ok(report) => DriverConformanceCaseResult::Passed(report),
        Err(failure) => DriverConformanceCaseResult::Failed(failure),
    }
}

/// Run the ordered portable host-driver corpus and retain every case result.
///
/// A setup or semantic failure in one fresh subject does not suppress later
/// independent cases. Use [`run_driver_conformance_case`] when the surrounding
/// test framework should isolate each case in its own test process boundary.
pub async fn run_driver_conformance(
    target: &dyn DriverConformanceTarget,
) -> DriverConformanceReport {
    let mut cases = Vec::with_capacity(DriverConformanceCase::ALL.len());
    for case in DriverConformanceCase::ALL {
        cases.push(run_driver_conformance_case(target, case).await);
    }
    DriverConformanceReport::new(target.name().to_owned(), target.kind(), cases)
}
