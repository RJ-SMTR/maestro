from dagster import pipeline
from dagster.core.definitions.mode import ModeDefinition

from repositories.maintenance.solids import (
    get_compare_timestamp,
    get_runs,
    filter_and_delete_runs,
)
from repositories.capturas.resources import discord_webhook, timezone_config
from repositories.helpers.hooks import (
    discord_message_on_failure,
    discord_message_on_success,
)


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
            resource_defs={
                "discord_webhook": discord_webhook,
                "timezone_config": timezone_config,
            },
        ),
    ],
    tags={
        "pipeline": "wipe_history",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "50m", "memory": "500Mi"},
                    "limits": {"cpu": "500m", "memory": "1Gi"},
                },
            }
        },
    },
)
def wipe_history():
    compare_timestamp = get_compare_timestamp()
    all_runs = get_runs()
    filter_and_delete_runs(all_runs, compare_timestamp)
