from dagster import resource, Field

@resource(
    {
        "timezone": Field(
            str, is_required=True, description="Run timezone"
        ),
    }
)
def timezone_config(context):
    return context.resource_config

@resource(
    {
        "url": Field(str, is_required=True, description="Discord webhook URL"),
        "success_cron": Field(str, is_required=False, description="Cron expression to post success hooks"),
    }
)
def discord_webhook(context):
    return context.resource_config