from dagster import resource, Field


@resource(
    {
        "timezone": Field(str, is_required=True, description="Run timezone"),
    }
)
def timezone_config(context):
    return context.resource_config


@resource(
    {
        "endpoints": Field(
            dict,
            is_required=True,
            description="dicts of endpoint and key_column keyed by table_id",
        )
    }
)
def endpoints(context):
    return context.resource_config

@resource({"map": Field(dict, is_required=True, description='column map from api response to project structure')})
def mapping(context):
    return context.resource_config


@resource(
    {
        "url": Field(str, is_required=True, description="Discord webhook URL"),
        "success_cron": Field(
            str, is_required=False, description="Cron expression to post success hooks"
        ),
    }
)
def discord_webhook(context):
    return context.resource_config


@resource(
    {
        "key": Field(
            str, is_required=True, description="Redis key for a particular pipeline"
        )
    }
)
def keepalive_key(context):
    return context.resource_config
