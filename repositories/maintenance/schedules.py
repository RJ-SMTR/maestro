from pathlib import Path
from datetime import datetime, time

from dagster import daily_schedule, ScheduleExecutionContext
from dagster.core.storage.event_log.schema import SqlEventLogStorageTable
from dagster.core.storage.runs.schema import RunsTable, RunTagsTable, SnapshotsTable

from repositories.helpers.helpers import read_config


@daily_schedule(
    pipeline_name='wipe_history',
    start_date=datetime(2021, 1, 1),
    name="wipe_history_daily",
    execution_time=time(1, 30),
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def wipe_history_daily(context: ScheduleExecutionContext):
    config = read_config(
        Path(__file__).parent / "wipe_history.yaml"
    )
    return config
