from datetime import datetime
import pendulum
from pottery.redlock import Redlock
import requests
from pathlib import Path
import shutil
import pandas as pd
from croniter import croniter
from redis import Redis
from redis_pal import RedisPal
from discord import Webhook, File, RequestsWebhookAdapter
from dagster import success_hook, failure_hook, HookContext

from repositories.helpers.constants import constants
from basedosdados import Storage


def post_message_to_discord(message, url):
    requests.post(url, data={"content": message})


def log_critical(message):
    post_message_to_discord(message, constants.CRITICAL_DISCORD_WEBHOOK.value)


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
        post_message_to_discord(message, url)


@failure_hook(required_resource_keys={"discord_webhook"})
def discord_message_on_failure(context: HookContext):
    message = f"@all Solid {context.solid.name} failed"
    url = context.resources.discord_webhook["url"]
    post_message_to_discord(message, url)


@success_hook(required_resource_keys={"keepalive_key"})
def redis_keepalive_on_succes(context: HookContext):
    rp = RedisPal(host=constants.REDIS_HOST.value)
    rp.set(context.resources.keepalive_key["key"], 1)


@failure_hook(required_resource_keys={"discord_webhook", "keepalive_key"})
def redis_keepalive_on_failure(context: HookContext):
    rp = RedisPal(host=constants.REDIS_HOST.value)
    rp.set(context.resources.keepalive_key["key"], 1)
    message = f"Although solid {context.solid.name} has failed, a keep-alive was sent to Redis!"
    url = context.resources.discord_webhook["url"]
    requests.post(url, data={"content": message})


@success_hook(required_resource_keys={"discord_webhook", "schedule_run_date"})
def stu_post_success(context: HookContext):
    ### Todas as classes referenciadas, são classes do Discord (Webhook, RequestsWebhookAdapter e File)
    run_date = context.resources.schedule_run_date["date"]
    filename = f"{run_date}/multas{run_date.replace('-','')}.csv"
    df = pd.read_csv(filename, sep=";", index_col=[0])
    message = f"""
    [Multas STU] Sumário {run_date} - Total: {df.shape[0]}
    """
    webhook = Webhook.from_url(
        url=context.resources.discord_webhook["url"],
        adapter=RequestsWebhookAdapter(),
    )
    with open(filename, "rb") as f:
        file = File(f)
    webhook.send(message, file=file, username="stu_hook")


@failure_hook(required_resource_keys={"basedosdados_config", "schedule_run_date"})
def stu_post_failure(context: HookContext):
    run_date = context.resources.schedule_run_date["date"]
    filename = Path(f"{run_date}/falha{run_date}.csv")
    content = {"data": run_date, "falha": True}

    df = pd.DataFrame(content, index=[0])
    df.to_csv(filename)

    st = Storage(
        context.resources.basedosdados_config["dataset_id"],
        context.resources.basedosdados_config["table_id"],
    )
    st.upload(filename, mode="staging")

    message = f"Solid {context.solid.name} failed. Uploading {filename} to {st.bucket_name}/staging/{st.dataset_id}/{st.table_id}"
    post_message_to_discord(message, url=context.resources.discord_webhook["url"])

    return shutil.rmtree(filename.parent)
