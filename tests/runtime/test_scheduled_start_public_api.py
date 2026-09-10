"""Public scheduled-start composition surface tests."""

from justflow import runtime
from justflow.runtime import (
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDecisionRecorder,
    ScheduledStartDescription,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartFailureCode,
    ScheduledStartMetricOutcome,
    ScheduledStartMutationResult,
    ScheduledStartMutationStatus,
    ScheduledStartPage,
    ScheduledStartQuotaController,
    ScheduledStartRescheduleRequest,
    ScheduledStartService,
    ScheduledStartState,
    ScheduledStartWorkloadClass,
    ScheduledStartWorkloadPolicy,
)


def test_scheduled_start_host_composition_types_are_public() -> None:
    public_types = {
        ScheduledStartCancelRequest,
        ScheduledStartCreateRequest,
        ScheduledStartDecisionRecorder,
        ScheduledStartDescription,
        ScheduledStartError,
        ScheduledStartErrorCode,
        ScheduledStartFailureCode,
        ScheduledStartMetricOutcome,
        ScheduledStartMutationResult,
        ScheduledStartMutationStatus,
        ScheduledStartPage,
        ScheduledStartQuotaController,
        ScheduledStartRescheduleRequest,
        ScheduledStartService,
        ScheduledStartState,
        ScheduledStartWorkloadClass,
        ScheduledStartWorkloadPolicy,
    }

    assert {public_type.__name__ for public_type in public_types} <= set(runtime.__all__)
