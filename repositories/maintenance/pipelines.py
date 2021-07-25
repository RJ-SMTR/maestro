from dagster import pipeline
from dagster.core.definitions.mode import ModeDefinition

from repositories.maintenance.solids import wipe_table_history, get_info_for_table, get_compare_timestamp
from repositories.capturas.resources import discord_webhook, timezone_config
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success


@discord_message_on_failure
@discord_message_on_success
@pipeline(mode_defs=[
    ModeDefinition(
        "dev", resource_defs={"discord_webhook": discord_webhook, "timezone_config": timezone_config}
    ),
],
)
def wipe_history():

    compare_timestamp = get_compare_timestamp()

    table_event_logs, compare_col_event_logs = get_info_for_table.alias(
        'get_info_for_event_logs')()
    wipe_table_history.alias('wipe_event_logs')(
        table_event_logs, compare_col_event_logs, compare_timestamp)

    table_runs, compare_col_runs = get_info_for_table.alias(
        'get_info_for_runs')()
    wipe_table_history.alias('wipe_runs')(
        table_runs, compare_col_runs, compare_timestamp)

    # wipe_table_history.alias('wipe_runs')(
    #     RunsTable, "create_timestamp", compare_timestamp)

    # TODO: RunTagsTable
    # TODO: SnapshotsTable
