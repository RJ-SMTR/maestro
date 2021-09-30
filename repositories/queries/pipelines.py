from dagster import pipeline
from dagster.core.definitions.mode import ModeDefinition

from repositories.capturas.resources import discord_webhook, timezone_config
from repositories.helpers.hooks import (
    discord_message_on_failure,
    discord_message_on_success,
)
from repositories.queries.solids import (
    delete_managed_views,
    gather_locks,
    update_managed_views,
    manage_view,
    resolve_dependencies_and_execute,
    get_configs_for_materialized_view,
    materialize,
    get_materialization_lock,
    get_materialize_sensor_lock,
    lock_materialization_process,
    release_materialization_process,
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
    lock = get_materialization_lock()
    locked = lock_materialization_process(lock)
    delete_managed_views(materialization_locked=locked,
                         materialization_lock=lock)
    runs = update_managed_views(
        materialization_locked=locked, materialization_lock=lock)
    done = runs.map(manage_view)
    release_materialization_process(lock, done.collect())


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
    lock = get_materialization_lock()
    lock_sensor = get_materialize_sensor_lock()
    locks = gather_locks(lock_sensor, lock)
    locked = lock_materialization_process(locks)
    views = resolve_dependencies_and_execute(
        materialization_locked=locked, materialization_lock=locks)
    configs = get_configs_for_materialized_view(
        views.collect(), materialization_locked=locked, materialization_lock=locks)
    done = configs.map(materialize)
    release_materialization_process([lock, lock_sensor], done.collect())
