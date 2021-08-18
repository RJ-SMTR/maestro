from dagster import pipeline
from dagster.core.definitions.mode import ModeDefinition

from repositories.capturas.resources import discord_webhook, timezone_config
from repositories.libraries.basedosdados.solids import update_view
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success
from repositories.queries.solids import (
    update_materialized_view_on_redis,
    resolve_dependencies_and_execute,
    get_configs_for_materialized_view,
    materialize
)


@discord_message_on_failure
@discord_message_on_success
@pipeline(mode_defs=[
    ModeDefinition(
        "dev", resource_defs={"discord_webhook": discord_webhook, "timezone_config": timezone_config}
    ),
],
)
def update_managed_materialized_views():
    update_materialized_view_on_redis()


@discord_message_on_failure
@discord_message_on_success
@pipeline(mode_defs=[
    ModeDefinition(
        "dev", resource_defs={"discord_webhook": discord_webhook, "timezone_config": timezone_config}
    ),
],
)
def materialize_view():
    views = resolve_dependencies_and_execute()
    configs = views.map(get_configs_for_materialized_view)
    configs.map(materialize)
