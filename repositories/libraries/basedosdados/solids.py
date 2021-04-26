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
from pathlib import Path
import basedosdados as bd

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



@solid(required_resource_keys={"basedosdados_config"})
def upload_to_bigquery(context, file_paths, partitions, modes=['raw', 'staging'], 
                       table_config='replace', publish_config='pass', is_init=False):
    if is_init:
        # Only available for mode staging
        try:
            idx = modes.index('staging')
            create_table_bq(context, file_paths[idx], table_config=table_config, publish_config=publish_config)
        except ValueError:
            raise RuntimeError("Publishing table outside staging mode")
    else:
        append_to_bigquery(context, file_paths, partitions, modes=modes)
        

def append_to_bigquery(context, file_paths, partitions, modes=['raw', 'staging']):

    table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    for idx, mode in enumerate(modes):
        context.log.info(f"Uploading to mode {mode}")
        st.upload(file_paths[idx], partitions=partitions, mode=mode, if_exists='replace')
        Path(file_paths[idx]).unlink(missing_ok=True)


def create_table_bq(context, file_path, table_config='replace', publish_config='pass'):

    table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    _file_path = file_path.split(table_id)[0] + table_id

    tb.create(
        path=Path(_file_path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists=table_config,
    )

    tb.publish(if_exists=publish_config)