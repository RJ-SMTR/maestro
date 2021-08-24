import time
from datetime import datetime, timedelta

from dagster import solid, SolidExecutionContext
from dagster.core.definitions import InputDefinition
from dagster.core.storage.pipeline_run import (
    PipelineRunsFilter,
    PipelineRunStatus,
)

from repositories.helpers.datetime import convert_unix_time_to_datetime


@solid
def get_runs(context: SolidExecutionContext):
    return context.instance.get_runs(
        filters=PipelineRunsFilter(
            statuses=[
                PipelineRunStatus.SUCCESS,
                PipelineRunStatus.FAILURE,
                PipelineRunStatus.CANCELED,
            ]
        )
    )


@solid
def filter_and_delete_runs(
    context: SolidExecutionContext, runs, compare_timestamp: datetime
):
    instance = context.instance
    total_runs = len(runs)
    deleted_runs = 0
    context.log.info(f"Collected {total_runs} runs")
    tick_delay = 3000 / (total_runs + 1)  # Make it last 50 minutes
    next_tick = time.time() + tick_delay
    i = 0
    while i < total_runs:
        while time.time() < next_tick:
            time.sleep(0.01)
        run_stats = instance.get_run_stats(runs[i].run_id)
        if run_stats is None:
            i += 1
            next_tick += tick_delay
            continue
        launch_time = run_stats.launch_time
        if launch_time is None:
            i += 1
            next_tick += tick_delay
            continue
        launch_time = convert_unix_time_to_datetime(launch_time)
        if launch_time <= compare_timestamp:
            instance.delete_run(runs[i].run_id)
            deleted_runs += 1
        next_tick += tick_delay
        i += 1
    context.log.info(f"Deleted {deleted_runs} out of {total_runs} runs")


@solid(
    input_defs=[
        InputDefinition("seconds", int, default_value=0),
        InputDefinition("minutes", int, default_value=0),
        InputDefinition("hours", int, default_value=0),
        InputDefinition("days", int, default_value=0),
    ]
)
def get_compare_timestamp(
    context: SolidExecutionContext, seconds, minutes, hours, days
) -> datetime:
    return datetime.now() - timedelta(
        seconds=seconds, minutes=minutes, hours=hours, days=days
    )

