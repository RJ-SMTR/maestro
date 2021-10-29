from datetime import datetime
import pendulum
from pottery.redlock import Redlock
import requests
import yagmail
from pathlib import Path
import shutil
import pandas as pd
from croniter import croniter
from redis import Redis
from redis_pal import RedisPal
from discord import Webhook, File, RequestsWebhookAdapter
from dagster import success_hook, failure_hook, HookContext
from repositories.helpers.constants import constants
from repositories.helpers.io import decode_str


def post_message_to_discord(message, url):
    requests.post(url, data={"content": message})


def log_critical(message):
    post_message_to_discord(message, constants.CRITICAL_DISCORD_WEBHOOK.value)


def post_to_discord_v2(url, username=None, message=None, filename=None):
    ### Todas as classes referenciadas, são classes do Discord (Webhook, RequestsWebhookAdapter e File)
    webhook = Webhook.from_url(url=url, adapter=RequestsWebhookAdapter())
    if filename:
        with open(filename, "rb") as f:
            file = File(f, filename=Path(filename).name)
        return webhook.send(content=message, username=username, file=file)
    else:
        return webhook.send(content=message, username=username)


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
    run_date = context.resources.schedule_run_date["date"]
    url = context.resources.discord_webhook["url"]
    filename = f"{run_date}/multas{run_date.replace('-','')}.csv"
    df = pd.read_csv(filename, sep=";", index_col=[0])
    message = f"""
    [Multas STU] Sumário {run_date} - Total: {df.shape[0]}
    """
    post_to_discord_v2(url=url, username="STU_hook", message=message, filename=filename)


@failure_hook(
    required_resource_keys={
        "discord_webhook",
        "schedule_run_date",
        "timezone_config",
    }
)
def stu_post_failure(context: HookContext):
    run_date = context.resources.schedule_run_date["date"]
    url = context.resources.discord_webhook["url"]
    dirname = Path(f"{run_date}")

    message = f"""
    #######   Solid {context.solid.name} run on {run_date} failed.   #######
    #######   Execution time: {pendulum.now(context.resources.timezone_config['timezone']).format("YYYY-MM-DD HH:mm:ss")}   #######
    """
    post_to_discord_v2(url=url, message=message, username="STU_hook")
    if dirname.is_dir():
        return shutil.rmtree(dirname)


@failure_hook(required_resource_keys={"automail_config", "schedule_run_date"})
def mail_failure(context: HookContext):
    content = context.resources.automail_config["content"][:]
    content[0] += f"{context.resources.schedule_run_date['date']}."
    return yagmail.SMTP(
        decode_str(context.resources.automail_config["from"]),
        decode_str(context.resources.automail_config["password"]),
    ).send(
        decode_str(context.resources.automail_config["to"]),
        context.resources.automail_config["subject"],
        content,
    )
