from dagster import pipeline, PipelineRun
from dagster.core.definitions.mode import ModeDefinition

from repositories.maintenance.solids import (
    get_compare_timestamp,
    get_dagster_instance,
    get_runs,
    filter_runs,
    delete_runs,
)
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
    instance = get_dagster_instance()
    all_runs = get_runs(instance)
    runs_to_wipe = filter_runs(instance, all_runs, compare_timestamp)
    delete_runs(instance, runs_to_wipe)
