"""Schedules.

Two rules are applied throughout.

**Nothing runs at midnight.** Every scheduler in every company runs everything
at 00:00, and the resulting thundering herd is why so many pipelines are slow
between midnight and one. Offsets are deliberate and staggered.

**Downstream waits for upstream by dependency, not by clock.** The warehouse
build is not scheduled fifteen minutes after ingestion and hoped for; it is
triggered by ingestion completing, which is what the sensors module is for.
Only genuinely independent work gets a cron.
"""

from __future__ import annotations

from dagster import (
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    build_schedule_from_partitioned_job,
    schedule,
)

from marketpulse_dagster.jobs import (
    daily_ingestion_job,
    maintenance_job,
    streaming_supervision_job,
    warehouse_build_job,
)

__all__ = [
    "daily_ingestion_schedule",
    "maintenance_schedule",
    "streaming_supervision_schedule",
    "warehouse_build_schedule",
]

# 00:20 UTC rather than 00:00: the last minute of the previous UTC day has to
# be fully landed by the streaming job (60-second trigger) plus a margin before
# the partition is complete. Scheduling at midnight would systematically drop
# the final bar of every day.
daily_ingestion_schedule = build_schedule_from_partitioned_job(
    daily_ingestion_job,
    hour_of_day=0,
    minute_of_hour=20,
    default_status=DefaultScheduleStatus.RUNNING,
)


@schedule(
    job=warehouse_build_job,
    cron_schedule="*/15 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Intraday warehouse refresh. Fifteen minutes matches the freshness "
        "check's threshold: refreshing faster than the SLA is wasted compute, "
        "slower guarantees the check fails."
    ),
)
def warehouse_build_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    return RunRequest(
        run_key=context.scheduled_execution_time.strftime("warehouse-%Y%m%dT%H%M"),
        tags={"trigger": "schedule", "cadence": "15m"},
    )


streaming_supervision_schedule = ScheduleDefinition(
    job=streaming_supervision_job,
    cron_schedule="*/10 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Ten minutes is the acceptable blind window for a stopped streaming query.",
)

maintenance_schedule = ScheduleDefinition(
    job=maintenance_job,
    # 03:40 UTC: past the daily ingestion window, well clear of the top of the
    # hour, and during the quietest stretch of the global trading day.
    cron_schedule="40 3 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Nightly Iceberg maintenance. Safe to skip a day; expensive to run at peak.",
)
