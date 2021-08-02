from datetime import datetime, timedelta

from dagster import DagsterInstance
from dagster import solid, SolidExecutionContext
from dagster.core.definitions import InputDefinition

from repositories.helpers.datetime import convert_unix_time_to_datetime


@solid
def get_dagster_instance(context) -> DagsterInstance:
    return DagsterInstance.get()


@solid
def get_runs(context, instance: DagsterInstance):
    return instance.get_runs()


@solid
def filter_runs(context, instance: DagsterInstance, runs, compare_timestamp: datetime):
    filtered_runs = []
    for run in runs:
        run_stats = instance.get_run_stats(run.run_id)
        if run_stats is None:
            context.log.warning(f'Failed to get run stats for ID {run.run_id}')
            continue
        launch_time = run_stats.launch_time
        if launch_time is None:
            context.log.warning(
                f'Failed to get enqueued time for ID {run.run_id}')
            continue
        launch_time = convert_unix_time_to_datetime(
            launch_time
        )
        if launch_time <= compare_timestamp:
            filtered_runs.append(run)
    return filtered_runs


@solid
def delete_runs(context, instance: DagsterInstance, runs):
    context.log.info(f'Will delete {len(runs)} runs')
    for run in runs:
        context.log.info(f'Deleting run {run.run_id}')
        instance.delete_run(run.run_id)


@solid(
    input_defs=[
        InputDefinition('seconds', int, default_value=0),
        InputDefinition('minutes', int, default_value=0),
        InputDefinition('hours', int, default_value=0),
        InputDefinition('days', int, default_value=0),
    ]
)
def get_compare_timestamp(context: SolidExecutionContext, seconds, minutes, hours, days) -> datetime:
    return datetime.now() - timedelta(seconds=seconds, minutes=minutes, hours=hours, days=days)