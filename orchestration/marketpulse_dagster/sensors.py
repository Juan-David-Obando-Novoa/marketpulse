"""Sensors: event-driven triggers and the alerting hook.

A sensor is the right tool exactly when the trigger is a *condition* rather
than a time. Scheduling the warehouse build fifteen minutes after ingestion and
hoping is the pattern these replace: it is wrong on a slow day and wasteful on
a fast one.
"""

from __future__ import annotations

from dagster import (
    AssetKey,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset_sensor,
    run_failure_sensor,
    sensor,
)

from marketpulse_dagster.jobs import streaming_supervision_job, warehouse_build_job

__all__ = [
    "bronze_landed_sensor",
    "pipeline_failure_sensor",
    "stalled_stream_sensor",
]


@asset_sensor(
    asset_key=AssetKey("bronze_reference_load"),
    job=warehouse_build_job,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Triggers the warehouse build when bronze actually lands, rather than "
        "on a clock offset that is wrong on a slow day and wasteful on a fast one."
    ),
)
def bronze_landed_sensor(context: SensorEvaluationContext, asset_event: object) -> RunRequest:
    return RunRequest(
        run_key=context.cursor,
        tags={"trigger": "bronze_landed"},
    )


@sensor(
    job=streaming_supervision_job,
    minimum_interval_seconds=120,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Restarts the streaming supervision job when bronze stops growing. "
        "Process liveness is not the signal -- a query can be running and "
        "landing nothing, which is precisely the failure that matters."
    ),
)
def stalled_stream_sensor(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    from marketpulse_dagster.resources import TrinoResource  # noqa: PLC0415

    trino = TrinoResource()
    try:
        latest = trino.scalar("select max(trade_time) from lakehouse.bronze.trades")
    except Exception as exc:  # noqa: BLE001 - a sensor must never crash the daemon
        return SkipReason(f"could not query bronze: {exc}")

    if latest is None:
        return SkipReason("bronze.trades is empty; nothing to compare against")

    marker = str(latest)
    if context.cursor == marker:
        # The high-water mark has not moved since the last evaluation, two
        # minutes ago. For a continuously traded instrument that is a stall.
        context.log.warning("bronze high-water mark unchanged at %s", marker)
        return SensorResult(
            run_requests=[RunRequest(run_key=f"stall-{marker}", tags={"trigger": "stall"})],
            cursor=marker,
        )

    return SensorResult(run_requests=[], cursor=marker)


@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING,
    description="Routes run failures to the platform's alerting channel with enough context to act.",
)
def pipeline_failure_sensor(context: RunFailureSensorContext) -> None:
    """Failure notification.

    Logs a structured summary rather than posting to a webhook, because the
    destination is deployment-specific and hard-coding one here would make the
    repository depend on somebody's Slack workspace. The shape of the message
    is the part worth committing: what failed, which partition, and the direct
    link -- an alert without those three is just a page.
    """
    run = context.dagster_run
    context.log.error(
        "pipeline failure | job=%s | run_id=%s | partition=%s | error=%s",
        run.job_name,
        run.run_id,
        run.tags.get("dagster/partition", "n/a"),
        (context.failure_event.message or "")[:1_000],
    )
