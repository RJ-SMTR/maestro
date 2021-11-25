from dagster import (
    solid,
    composite_solid,
    InputDefinition,
    OutputDefinition,
    SolidExecutionContext,
    Nothing,
    Field,
    RetryPolicy,
)

from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from pathlib import Path
import basedosdados as bd
from basedosdados import Table, Storage
from repositories.libraries.jinja2.solids import render
from repositories.helpers.io import get_credentials_from_env


# @solid(retry_policy=RetryPolicy(max_retries=3, delay=5))
# def update_view(
#     context: SolidExecutionContext, view_sql: str, table_name: str, delete: bool = False
# ) -> Nothing:

#     # Table ID can't be empty
#     if table_name is None or table_name == "":
#         raise Exception("Table name can't be None or empty!")

#     # SQL can't be empty if not removing table
#     if (view_sql is None or view_sql == "") and (not delete):
#         raise Exception("Query can't be None or empty!")

#     # Setup credentials and BQ client
#     credentials = get_credentials_from_env()
#     client = bigquery.Client(credentials=credentials)

#     # Delete
#     if delete:
#         client.delete_table(table_name, not_found_ok=True)

#     # Create/update
#     else:
#         table = bigquery.Table(table_name)
#         table.view_query = view_sql

#         # Always Overwrite
#         try:
#             client.create_table(table)
#         except Conflict:
#             client.delete_table(table)
#             client.create_table(table)


def config_mapping_fn(config):

    return {
        "update_view": {
            "config": {
                "view_sql": config["view_sql"],
                "table_name": config["table_name"],
                "delete": config["delete"],
            }
        },
        "render": {
            "config": {
                "context": config["sql_context"],
                "filepath": config["sql_filepath"],
            }
        },
    }
@solid(required_resource_keys={'basedosdados_config'})
def bq_upload(context, filepath,raw_filepath=None, partitions=None):
    table_id = context.resources.basedosdados_config['table_id']
    dataset_id=context.resources.basedosdados_config['dataset_id']
    context.log.info(f"""
    Received inputs:
    raw_filepath = {raw_filepath}, type = {type(raw_filepath)}
    treated_filepath = {filepath}, type = {type(filepath)}
    dataset_id = {dataset_id}, type = {type(dataset_id)}
    table_id = {table_id}, type = {type(table_id)}
    partitions = {partitions}, type = {type(partitions)}
    """)
    # Upload raw to staging
    if raw_filepath:
        st = Storage(
            table_id = table_id,
            dataset_id=dataset_id
        )
        context.log.info(f"Uploading raw file: {raw_filepath} to bucket {st.bucket_name} at {st.bucket_name}/{dataset_id}/{table_id}")
        st.upload(path=raw_filepath, partitions=partitions, mode='raw', if_exists='replace')
    
    # creates and publish table if it does not exist, append to it otherwise
    if partitions:
        # If table is partitioned, get parent directory wherein partitions are stored
        tb_dir = filepath.split(partitions)[0]
        create_or_append_table(context, dataset_id, table_id, tb_dir)
    else:
        create_or_append_table(context, dataset_id, table_id, filepath)

    # Delete local Files
    context.log.info(f"Deleting local files: {raw_filepath}, {filepath}")
    cleanup_local(filepath, raw_filepath)

def create_or_append_table(context, dataset_id, table_id, path):
    tb = Table(
        table_id = table_id,
        dataset_id= dataset_id
    )
    if not tb.table_exists('staging'):
        context.log.info(
                "Table does not exist in STAGING, creating table...")
        tb.create(
            path=path,
            if_table_exists="pass",
            if_storage_data_exists="replace",
            if_table_config_exists="pass",
        )
        context.log.info("Table created in STAGING")
    else:
        context.log.info(
            "Table already exists in STAGING, appending to it...")
        tb.append(
            filepath=path, 
            if_exists="replace",
            timeout=600,
            chunk_size=1024*1024*10)
        context.log.info("Appended to table on STAGING successfully.")

    if not tb.table_exists("prod"):
        context.log.info("Table does not exist in PROD, publishing...")
        tb.publish(if_exists="pass")
        context.log.info("Published table in PROD successfully.")
    else:
        context.log.info("Table already published in PROD.")

def cleanup_local(filepath, raw_filepath=None):
    if raw_filepath:
        Path(raw_filepath).unlink(missing_ok=True)
    Path(filepath).unlink(missing_ok=True)

@solid(
    required_resource_keys={"basedosdados_config"},
    config_schema={
        "mode": str,
        "table_config": str,
        "publish_config": str,
        "is_init": bool,
    },
)
def upload_to_bigquery_v2(context, file_path, partitions=None, table_id=None):

    if context.solid_config["is_init"]:
        # Only available for mode staging
        try:
            create_table_bq_v2(
                context,
                file_path,
                table_config=context.solid_config["table_config"],
                publish_config=context.solid_config["publish_config"],
                table_id=table_id,
            )
        except ValueError:
            raise RuntimeError("Publishing table outside staging mode")
    else:
        append_to_bigquery_v2(
            context,
            file_path,
            partitions,
            mode=context.solid_config["mode"],
            table_id=table_id,
        )


def append_to_bigquery_v2(context, file_path, partitions, mode, table_id=None):

    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    bd.Storage(dataset_id=dataset_id, table_id=table_id).upload(
        file_path, partitions=partitions, mode=mode, if_exists="replace"
    )

    # delete file
    Path(file_path).unlink(missing_ok=True)


def create_table_bq_v2(
    context, file_path, table_config="replace", publish_config="pass", table_id=None
):
    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    context.log.debug(f"Filepath: {file_path}")

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    tb.create(
        path=Path(file_path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists=table_config,
    )

    tb.publish(if_exists=publish_config)

    # delete file
    Path(file_path).unlink(missing_ok=True)


@solid(required_resource_keys={"basedosdados_config"})
def upload_to_bigquery(
    context,
    file_paths,
    partitions,
    modes=["raw", "staging"],
    table_config="replace",
    publish_config="pass",
    is_init=False,
    table_id=None,
):
    if is_init:
        # Only available for mode staging
        try:
            idx = modes.index("staging")
            create_table_bq(
                context,
                file_paths[idx],
                table_config=table_config,
                publish_config=publish_config,
                table_id=table_id,
            )
        except ValueError:
            raise RuntimeError("Publishing table outside staging mode")
    else:
        append_to_bigquery(
            context, file_paths, partitions, modes=modes, table_id=table_id
        )


def append_to_bigquery(
    context,
    file_paths,
    partitions,
    modes=["raw", "staging"],
    table_id=None,
    dataset_id=None,
):

    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
    if not dataset_id:
        dataset_id = context.resources.basedosdados_config["dataset_id"]

    context.log.info(f"Table ID: {table_id} / Dataset ID: {dataset_id}")

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    for idx, mode in enumerate(modes):
        context.log.info(
            f"Uploading file {file_paths[idx]} to mode {mode} with partitions {partitions}"
        )
        st.upload(
            file_paths[idx], partitions=partitions, mode=mode, if_exists="replace"
        )
        Path(file_paths[idx]).unlink(missing_ok=True)


def create_table_bq(
    context,
    file_path,
    table_config="replace",
    publish_config="pass",
    table_id=None,
    dataset_id=None,
):

    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
    if not dataset_id:
        dataset_id = context.resources.basedosdados_config["dataset_id"]

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    _file_path = file_path.split(table_id)[0] + table_id
    context.log.debug(_file_path)
    context.log.debug(table_id)

    tb.create(
        path=Path(_file_path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists=table_config,
    )

    tb.publish(if_exists=publish_config)
