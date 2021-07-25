from datetime import datetime, timedelta

import sqlalchemy as db
from dagster import Output
from dagster import solid, SolidExecutionContext
from dagster.core.definitions import InputDefinition, OutputDefinition
from dagster.core.storage.event_log.schema import SqlEventLogStorageTable
from dagster.core.storage.runs.schema import RunsTable, RunTagsTable, SnapshotsTable

from repositories.helpers.io import get_session_builder


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


@ solid(
    output_defs=[
        OutputDefinition(name="table"),
        OutputDefinition(name="compare_col"),
    ],

)
def get_info_for_table(context: SolidExecutionContext, table_name: str):
    if table_name == "event_log":
        yield Output(SqlEventLogStorageTable, output_name="table")
        yield Output("timestamp", output_name="compare_col")
    elif table_name == "runs":
        yield Output(RunsTable, output_name="table")
        yield Output("create_timestamp", output_name="compare_col")


@ solid
def wipe_table_history(context: SolidExecutionContext, table: db.Table, compare_col: str, compare_timestamp: datetime):
    Session = get_session_builder()
    session: db.orm.session.Session = Session()
    deleted = session.query(table).filter(getattr(table.c, compare_col)
                                          < compare_timestamp).delete()
    context.log.info(f'Deleted {deleted} rows from {table.name}')
    session.commit()
    session.close()
