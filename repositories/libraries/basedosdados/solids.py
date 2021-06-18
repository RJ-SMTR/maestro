from dagster import (
    solid,
    composite_solid,
    InputDefinition,
    OutputDefinition,
    SolidExecutionContext,
    Nothing,
    Field,
)

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict
from pathlib import Path
import basedosdados as bd

from repositories.libraries.jinja2.solids import render


@solid
def update_view(context: SolidExecutionContext, view_sql: str, table_name: str, delete: bool = False) -> Nothing:

    # Table ID can't be empty
    if table_name is None or table_name == "":
        raise Exception("Table name can't be None or empty!")

    # SQL can't be empty if not removing table
    if (view_sql is None or view_sql == "") and (not delete):
        raise Exception("Query can't be None or empty!")

    # Setup credentials and BQ client
    credentials = service_account.Credentials.from_service_account_file(
        Path.home() / ".basedosdados/credentials/prod.json")
    client = bigquery.Client(credentials=credentials)

    # Delete
    if (delete):
        client.delete_table(table_name, not_found_ok=True)

    # Create/update
    else:
        table = bigquery.Table(table_name)
        table.view_query = view_sql

        # Always Overwrite
        try:
            client.create_table(table)
        except Conflict:
            client.delete_table(table)
            client.create_table(table)


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
    context, file_paths, partitions, modes=["raw", "staging"], table_id=None
):

    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    for idx, mode in enumerate(modes):
        context.log.info(f"Uploading to mode {mode}")
        st.upload(
            file_paths[idx], partitions=partitions, mode=mode, if_exists="replace"
        )
        Path(file_paths[idx]).unlink(missing_ok=True)


def create_table_bq(
    context, file_path, table_config="replace", publish_config="pass", table_id=None
):

    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
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
