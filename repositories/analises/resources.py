from dagster import resource, Field


@resource(
    {
        "dataset_id": Field(
            str, is_required=True, description="dataset_id for gtfs version"
        ),
        "table_id": Field(str, is_required=True, description="table_id for gtfs"),
        "storage_path": Field(
            str,
            is_required=True,
            description="Path from which to download gtfs versions from storage",
        ),
    }
)
def gtfs(context):
    return context.resource_config


@resource(
    {
        "query_table": Field(
            str, is_required=True, description="Table to query for raw gps data"
        )
    }
)
def gps_data(context):
    return context.resource_config
