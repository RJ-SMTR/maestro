from dagster import pipeline
from dagster.core.definitions.mode import ModeDefinition

from repositories.capturas.resources import discord_webhook, timezone_config
from repositories.libraries.basedosdados.solids import update_view
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success


@discord_message_on_failure
@discord_message_on_success
@pipeline(mode_defs=[
    ModeDefinition(
        "dev", resource_defs={"discord_webhook": discord_webhook, "timezone_config": timezone_config}
    ),
],
)
def update_view_on_bigquery():
    update_view()
