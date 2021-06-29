from dagster import resource, Field


@resource(
    {"date": Field(str, is_required=True, description="Date partition from schedule")}
)
def schedule_run_date(context):
    return context.resource_config
