from dagster import success_hook, failure_hook, HookContext
import pendulum
import requests
import pandas as pd
from basedosdados.table import Table
from croniter import croniter
from datetime import datetime
from redis_pal import RedisPal
from repositories.libraries.basedosdados.solids import (
    append_to_bigquery_v2,
    create_table_bq_v2,
)


@success_hook(required_resource_keys={"discord_webhook", "timezone_config"})
def discord_message_on_success(context: HookContext):
    timezone = context.resources.timezone_config["timezone"]
    cron_expression = context.resources.discord_webhook["success_cron"]
    run_time = pendulum.now(timezone)
    cron_itr = croniter(cron_expression, run_time)
    post_time = cron_itr.get_prev(datetime)
    if run_time.strftime("%Y-%m-%d %H:%M") == post_time.strftime("%Y-%m-%d %H:%M"):
        message = f"Solid {context.solid.name} finished successfully"
        url = context.resources.discord_webhook["url"]
        requests.post(url, data={"content": message})


@failure_hook(required_resource_keys={"discord_webhook"})
def discord_message_on_failure(context: HookContext):
    message = f"Solid {context.solid.name} failed"
    url = context.resources.discord_webhook["url"]
    requests.post(url, data={"content": message})


@success_hook(required_resource_keys={"keepalive_key"})
def redis_keepalive_on_succes(context: HookContext):
    rp = RedisPal(host="redis")
    rp.set(context.resources.keepalive_key["key"], 1)


@failure_hook(required_resource_keys={"discord_webhook", "keepalive_key"})
def redis_keepalive_on_failure(context: HookContext):
    rp = RedisPal(host="redis")
    rp.set(context.resources.keepalive_key["key"], 1)
    message = f"Although solid {context.solid.name} has failed, a keep-alive was sent to Redis!"
    url = context.resources.discord_webhook["url"]
    requests.post(url, data={"content": message})


def upload_logs_to_bq(dataset_id, table_id, filepath, table_config):
    tb = Table(table_id, dataset_id)
    tb.create(
        path=filepath,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists=table_config,
    )
    tb.publish(if_exists="pass")


@success_hook(required_resource_keys={"basedosdados_config"})
def upload_success_to_BQ(context: HookContext):

    df = pd.DataFrame(
        {"timestamp_captura": [pendulum.now()], "sucesso": [True], "erro": [""]}
    )
    filepath = f"success_hook_{pendulum.now()}.csv"
    df.to_csv(filepath, index=False)
    upload_logs_to_bq(
        dataset_id=context.resources.basedosdados_config["dataset_id"],
        table_id=context.resources.basedosdados_config["table_id"] + "_logs",
        filepath=filepath,
        table_config="pass",
    )


@failure_hook(required_resource_keys={"basedosdados_config"})
def upload_failure_to_BQ(context: HookContext):
    df = pd.DataFrame(
        {
            "timestamp_captura": [pendulum.now()],
            "sucesso": [False],
            "erro": [context.solid_exception],
        }
    )
    filepath = f"failure_hook_{pendulum.now()}.csv"
    df.to_csv(filepath, index=False)
    upload_logs_to_bq(
        dataset_id=context.resources.basedosdados_config["dataset_id"],
        table_id=context.resources.basedosdados_config["table_id"] + "_logs",
        filepath=filepath,
        table_config="replace",
    )
