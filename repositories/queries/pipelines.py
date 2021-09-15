from dagster import pipeline
from dagster.core.definitions.mode import ModeDefinition

from repositories.capturas.resources import discord_webhook, timezone_config
from repositories.helpers.hooks import (
    discord_message_on_failure,
    discord_message_on_success,
)
from repositories.queries.solids import (
    delete_managed_views,
    update_managed_views,
    manage_view,
    resolve_dependencies_and_execute,
    get_configs_for_materialized_view,
    materialize,
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
        "pipeline": "update_managed_materialized_views",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "50m", "memory": "100Mi"},
                    "limits": {"cpu": "500m", "memory": "1Gi"},
                },
            }
        },
    },
)
def update_managed_materialized_views():
    delete_managed_views()
    runs = update_managed_views()
    runs.map(manage_view)


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
        "pipeline": "materialize_view",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "50m", "memory": "100Mi"},
                    "limits": {"cpu": "500m", "memory": "1Gi"},
                },
            }
        },
    },
)
def materialize_view():
    views = resolve_dependencies_and_execute()
    configs = get_configs_for_materialized_view(views.collect())
    configs.map(materialize)
