from dagster import success_hook, failure_hook, HookContext
import pendulum
import requests
from croniter import croniter
from datetime import datetime
from redis_pal import RedisPal


@success_hook(required_resource_keys={"discord_webhook", "timezone_config"})
def discord_message_on_success(context: HookContext):
    timezone = context.resources.timezone_config["timezone"]
    cron_expression = context.resources.discord_webhook["success_cron"]
    run_time = pendulum.now(timezone)
    cron_itr = croniter(cron_expression, run_time)
    post_time = cron_itr.get_prev(datetime)
    if run_time.strftime("%Y-%m-%d %H:%M") == post_time.strftime("%Y-%m-%d %H:%M"):
        message = f"Solid {context.solid.name} finished successfully"
        url = context.resources.discord_webhook['url']
        requests.post(url, data={'content': message})


@failure_hook(required_resource_keys={"discord_webhook"})
def discord_message_on_failure(context: HookContext):
    message = f"Solid {context.solid.name} failed"
    url = context.resources.discord_webhook['url']
    requests.post(url, data={'content': message})


@success_hook(required_resource_keys={"keepalive_key"})
def redis_keepalive_on_succes(context: HookContext):
    rp = RedisPal(host="dagster-redis-master")
    rp.set(context.resources.keepalive_key['key'], 1)


@failure_hook(required_resource_keys={"discord_webhook", "keepalive_key"})
def redis_keepalive_on_failure(context: HookContext):
    rp = RedisPal(host="dagster-redis-master")
    rp.set(context.resources.keepalive_key['key'], 1)
    message = f"Although solid {context.solid.name} has failed, a keep-alive was sent to Redis!"
    url = context.resources.discord_webhook['url']
    requests.post(url, data={'content': message})
