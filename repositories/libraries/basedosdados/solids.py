from dagster import (
    solid,
    composite_solid,
    InputDefinition,
    OutputDefinition,
    Nothing,
    Field,
)

from google.cloud import bigquery
from google.api_core.exceptions import Conflict

from repositories.libraries.jinja2.solids import render


@solid(
    input_defs=[InputDefinition("view_sql", str)],
    output_defs=[OutputDefinition(Nothing)],
    config_schema={
        "dataset_id": Field(str, is_required=True),
        "table_id": Field(str, is_required=True),
    },
    required_resource_keys={"bd_client"},
)
def create_view(context, view_sql):

    client = context.resources.bd_client

    project_id = client.project
    dataset_id = context.solid_config["dataset_id"]
    table_id = context.solid_config["table_id"]

    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}")
    table.view_query = view_sql

    # Always Overwrite
    try:
        client.create_table(table)
    except Conflict:
        client.delete_table(table)
        client.create_table(table)


def config_mapping_fn(config):

    return {
        "create_view": {
            "config": {
                "dataset_id": config["dataset_id"],
                "table_id": config["table_id"],
            }
        },
        "render": {
            "config": {
                "context": config["sql_context"],
                "filepath": config["sql_filepath"],
            }
        },
    }


@composite_solid(
    config_fn=config_mapping_fn,
    config_schema={
        "dataset_id": str,
        "table_id": str,
        "sql_filepath": str,
        "sql_context": dict,
    },
    input_defs=[InputDefinition("_", Nothing)],
)
def render_and_create_view(_):

    sql = render(_)
    return create_view(sql)
