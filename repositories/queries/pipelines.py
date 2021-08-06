from dagster import pipeline
from dagster.core.definitions.mode import ModeDefinition

from repositories.capturas.resources import discord_webhook, timezone_config
from repositories.libraries.basedosdados.solids import update_view
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success
from repositories.queries.solids import (
    update_materialized_view_on_redis,
    create_table_if_not_exists,
    delete_table_on_query_change,
    insert_into_table_if_already_existed,
)


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

    # Delete table if query has changed
    # Steps:
    # 1. Check if table exists
    # 2. If exists, check if query has changed
    # 3. If changed, delete table
    done_1 = delete_table_on_query_change()

    # Create table and run query if table does not exist
    # Steps:
    # - If table exists, return
    # - If table does not exist, create table
    already_existed = create_table_if_not_exists(last_step_done=done_1)

    # Run query if table already existed
    # Steps:
    # - If table exists, run query
    # - If table does not exist, return
    insert_into_table_if_already_existed(already_existed=already_existed)
