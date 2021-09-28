from pathlib import Path
from datetime import datetime, time

from dagster import schedule, ScheduleExecutionContext
from dagster.core.storage.event_log.schema import SqlEventLogStorageTable
from dagster.core.storage.runs.schema import RunsTable, RunTagsTable, SnapshotsTable

from repositories.helpers.helpers import read_config


@schedule(
    cron_schedule="0 * * * *",
    pipeline_name="wipe_history",
    name="wipe_history_hourly",
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def wipe_history_hourly(context: ScheduleExecutionContext):
    config = read_config(Path(__file__).parent / "wipe_history.yaml")
    return config
