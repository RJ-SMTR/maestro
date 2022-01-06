import os
import re
import uuid
from pathlib import Path
from datetime import datetime, timedelta

import jinja2
import pendulum
import pandas as pd
import basedosdados as bd
from dateutil import parser
from dagster import solid, pipeline, ModeDefinition, PresetDefinition, RetryPolicy
from dagster.experimental import DynamicOutputDefinition, DynamicOutput

from repositories.capturas.resources import (
    timezone_config,
    discord_webhook,
)
from repositories.libraries.basedosdados.resources import basedosdados_config
from repositories.helpers.datetime import convert_datetime_to_unix_time
from repositories.helpers.helpers import read_config
from repositories.helpers.hooks import (
    discord_message_on_failure,
    discord_message_on_success,
    redis_keepalive_on_failure,
    redis_keepalive_on_succes,
)
from repositories.helpers.io import connect_ftp
from repositories.libraries.basedosdados.solids import (
    append_to_bigquery,
    create_table_bq,
)
from repositories.helpers.storage import StoragePlus

ALLOWED_FOLDERS = ["SPPO", "STPL"]
FTPS_DIRECTORY = os.getenv("FTPS_DATA", "/opt/dagster/app/data/FTPS_DATA")


def fn_download_file_from_ftp(context, ftp_path: str, local_path: str) -> None:
    """Downloads a file from FTP to the local storage"""
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    ftp_client = connect_ftp(
        os.getenv("FTPS_HOST"), os.getenv("FTPS_USERNAME"), os.getenv("FTPS_PWD")
    )
    context.log.info(f"Downloading file from FTP: {ftp_path}")
    ftp_client.retrbinary("RETR " + ftp_path, open(local_path, "wb").write)
    ftp_client.quit()


def fn_parse_file_path(context, config: dict, table_id: str, dataset_id: str):
    """
    Parse file path on dict to get the file name, type, path and
    partitions as new keys on dict (config).
    """
    # Parse bucket path to get filename & type
    path_list = config["bucket_path"].split("/")
    # Get data folder from environment variable
    file_path = f"{os.getcwd()}/{os.getenv('DATA_FOLDER', 'data')}/{{mode}}/{dataset_id}/{table_id}/"
    # Parse bucket to get partitions

    if config["partitions"]:
        partitions = re.findall("\/([^\/]*?)=(.*?)(?=\/)", config["bucket_path"])
        config["partitions"] = "/".join(
            ["=".join([field for field in item]) for item in partitions]
        )
        file_path += f"{config['partitions']}/"
    # Set upload parameters
    config["filename"] = path_list[-1].split(".")[0]
    config["file_type"] = path_list[-1].split(".")[1]
    config["file_path"] = file_path + f'{config["filename"]}.{{file_type}}'
    context.log.info(f"Creating file path: {config['file_path']}")
    return config


def fn_upload_file_to_storage(
    context,
    local_path: str,
    table_id: str,
    dataset_id: str,
    partitions: list = None,
    mode: str = "raw",
):
    # Upload to GCS
    st = bd.Storage(table_id=table_id, dataset_id=dataset_id)
    context.log.debug(
        f"Uploading file {local_path} to {mode}/{dataset_id}/{table_id} with partitions {partitions}"
    )
    st.upload(
        path=local_path,
        mode=mode,
        partitions=partitions if partitions else None,
        if_exists="replace",
    )
    return True


def fn_get_file_from_storage(
    context,
    file_path: str,
    filename: str,
    file_type: str,
    partitions: str = None,
    mode: str = "raw",
    uploaded: bool = True,
    table_id: str = None,
    dataset_id: str = None,
):
    # Set paths and iniciate
    _file_path = file_path.format(mode=mode, file_type=file_type)
    st = StoragePlus(table_id=table_id, dataset_id=dataset_id)
    context.log.info(
        f"Downloading file from storage: {mode}/{dataset_id}/{table_id}/{filename}.{file_type}"
    )
    # Download file from GCS
    st.download(
        filename=filename + "." + file_type,
        file_path=_file_path,
        partitions=partitions if partitions else None,
        mode=mode,
        if_exists="replace",
    )
    return _file_path


def _fn_aux_get_list(context, file_path: str, read_csv_kwargs: dict, transform: dict):
    """
    Read auxiliar file and return a list of values from it. Used to get
    the list of permission codes from bus and vans.
    """
    df = pd.read_csv(file_path, **read_csv_kwargs)
    for col, value in transform["filter"].items():
        df[col] = df[col].str.strip()
        df = df[df[col] == value]
    return dict(df[transform["column_to_dict"]])


def fn_load_and_transform_csv(
    context,
    file_path: str,
    read_csv_kwargs: dict,
    reindex_columns: list,
    map_columns: dict = None,
    map_values: dict = None,
    reorder_columns: dict = None,
):
    df = pd.read_csv(file_path, header=None, **read_csv_kwargs)
    # Set column names for those already in the file
    df.columns = reindex_columns[: len(df.columns)]
    # Map new columns to values
    for col, mapped_col in map_columns.items():
        df[mapped_col] = df[col].map(map_values)
    # Return ordered columns
    if reorder_columns:
        ordered = [
            reorder_columns[col] if col in reorder_columns.keys() else i
            for i, col in enumerate(reindex_columns)
        ]
        cols = [reindex_columns[col] for col in ordered]
    else:
        cols = reindex_columns
    return df[cols]


def fn_add_timestamp(context, df: pd.DataFrame):
    timezone = context.resources.timezone_config["timezone"]
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    context.log.debug(f"Add timestamp column. Final columns: {df.columns}")
    return df


def fn_divide_columns(df: pd.DataFrame, cols_to_divide: list = None, value: int = 100):
    if cols_to_divide:
        # Divide columns by value
        df[cols_to_divide] = df[cols_to_divide].apply(lambda x: x / value, axis=1)
    return df


def fn_upload_to_datalake(
    context,
    table_id: str,
    dataset_id: str,
    file_paths: list,
    partitions: list = None,
    modes: list = ["raw", "staging"],
    table_config: str = "replace",
    publish_config: str = "pass",
    is_init: bool = False,
):
    # Upload to BigQuery
    if is_init:
        # Only available for mode staging
        if "staging" in modes:
            idx = modes.index("staging")
            context.log.info(f"Publishing table to BQ: {dataset_id}.{table_id}")
            create_table_bq(
                context,
                file_paths[idx],
                table_config=table_config,
                publish_config=publish_config,
                table_id=table_id,
                dataset_id=dataset_id,
            )
        else:
            raise RuntimeError("Publishing table outside staging mode")
    else:
        append_to_bigquery(
            context,
            file_paths,
            partitions,
            modes=modes,
            table_id=table_id,
            dataset_id=dataset_id,
        )


def fn_save_treated_local(
    context, df: pd.DataFrame, file_path: str, file_type: str, mode="staging"
):

    _file_path = file_path.format(mode=mode, file_type=file_type)
    _file_path = Path(_file_path)
    _file_path.parent.mkdir(parents=True, exist_ok=True)
    _file_path = str(_file_path)
    context.log.info(f"Saving df to: {_file_path}")
    df.to_csv(_file_path, index=False)
    return _file_path


@solid(
    output_defs=[DynamicOutputDefinition(dict)],
    required_resource_keys={"timezone_config", "basedosdados_config"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def get_runs(context, execution_date):
    # Define date constants
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    now = execution_date + timedelta(hours=11, minutes=30)
    min_timestamp = convert_datetime_to_unix_time(now - timedelta(days=1))
    max_timestamp = convert_datetime_to_unix_time(now)
    context.log.info(f"{execution_date} of type {type(execution_date)}")
    # Connect to FTP
    ftp_client = connect_ftp(
        os.getenv("FTPS_HOST"), os.getenv("FTPS_USERNAME"), os.getenv("FTPS_PWD")
    )
    # Change to working directory
    ftp_client.cwd("/")
    for folder in ftp_client.mlsd():
        # Config yaml file will be folder_fileprefix.yaml
        if folder[1]["type"] == "dir" and folder[0] in ALLOWED_FOLDERS:
            # CWD to folder
            context.log.info(f"Entering folder: {folder[0]}")
            folder_name = folder[0].lower()
            # Read file list
            for filepath in ftp_client.mlsd(folder_name):
                filename = filepath[0]
                fileprefix = filename.split("_")[0].lower()
                file_mtime = datetime.timestamp(parser.parse(filepath[1]["modify"]))

                if file_mtime >= min_timestamp and file_mtime < max_timestamp:
                    context.log.info(
                        f"Found file {filename}" + " with timestamp " + str(file_mtime)
                    )
                    # Get config for modal
                    config = read_config(
                        Path(__file__).parent / f"{folder_name}_{fileprefix}.yaml"
                    )
                    # Set global variables
                    # Get file paths
                    date = tuple(re.findall("\d+", filename))
                    ano = date[2][:4]
                    mes = date[2][4:6]
                    dia = date[2][6:]
                    dataset_id = config["resources"]["basedosdados_config"]["config"][
                        "dataset_id"
                    ]
                    table_id = config["resources"]["basedosdados_config"]["config"][
                        "table_id"
                    ]
                    relative_filepath = Path(
                        "raw",
                        dataset_id,
                        table_id,
                        f"ano={ano}",
                        f"mes={mes}",
                        f"dia={dia}",
                    )
                    local_filepath = Path(FTPS_DIRECTORY, relative_filepath)
                    Path(local_filepath).mkdir(parents=True, exist_ok=True)
                    # Set config paths to execute pipeline
                    config["solids"]["paths"] = {
                        "ftp_path": str(Path(folder_name, filename)),
                        "local_path": str(Path(local_filepath, filename)),
                        "bucket_path": f"{relative_filepath}/{filename}",
                        "partitions": config["solids"]["paths"]["partitions"],
                        "file_path": str(Path(local_filepath, filename)),
                        "dataset_id": dataset_id,
                        "table_id": table_id,
                    }
                    context.log.info(f"Set config paths: {config['solids']['paths']}")
                    yield DynamicOutput(
                        config,
                        mapping_key=f"{folder_name}_{fileprefix}_{uuid.uuid4().hex}",
                    )
                ftp_client.cwd("/")
        else:
            context.log.warning(
                f"Skipping file {folder[0]} since it is not inside a folder"
            )
            continue


@solid(
    required_resource_keys={"timezone_config", "basedosdados_config"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def execute_run(context, run_config: dict):
    # Get fixed paths
    config = run_config["solids"]

    # Get file from FTPS and save it locally
    fn_download_file_from_ftp(
        context,
        ftp_path=config["paths"]["ftp_path"],
        local_path=config["paths"]["local_path"],
    )
    # Parse file path and get partitions
    config["paths"] = fn_parse_file_path(
        context,
        config=config["paths"],
        table_id=config["paths"]["table_id"],
        dataset_id=config["paths"]["dataset_id"],
    )
    # Upload file to GCS
    uploaded = fn_upload_file_to_storage(
        context,
        table_id=config["paths"]["table_id"],
        dataset_id=config["paths"]["dataset_id"],
        local_path=config["paths"]["local_path"],
        partitions=config["paths"]["partitions"],
    )
    # Get data from GCS
    raw_file_path = fn_get_file_from_storage(
        context,
        file_path=config["paths"]["file_path"],
        filename=config["paths"]["filename"],
        file_type=config["paths"]["file_type"],
        partitions=config["paths"]["partitions"],
        uploaded=uploaded,
        table_id=config["paths"]["table_id"],
        dataset_id=config["paths"]["dataset_id"],
    )
    # Get auxilar file from GCS
    aux_config = config["aux_get_list"]
    context.log.info(f"Get auxiliar file. Aux config: {aux_config}")
    # Parse file path and get partitions
    aux_config["paths"] = fn_parse_file_path(
        context,
        config=aux_config["paths"],
        table_id=aux_config["paths"]["table_id"],
        dataset_id=aux_config["paths"]["dataset_id"],
    )
    # Extract aux list
    aux_code_list = _fn_aux_get_list(
        context,
        file_path=fn_get_file_from_storage(
            context,
            file_path=aux_config["paths"]["file_path"],
            filename=aux_config["paths"]["filename"],
            file_type=aux_config["paths"]["file_type"],
            partitions=aux_config["paths"]["partitions"],
            uploaded=True,
            table_id=aux_config["paths"]["table_id"],
            dataset_id=aux_config["paths"]["dataset_id"],
        ),
        read_csv_kwargs=aux_config["read_csv_kwargs"],
        transform=aux_config["transform"],
    )
    # Extract, load and transform data
    treat_config = config["load_and_transform_csv"]
    treated_data = fn_add_timestamp(
        context,
        df=fn_load_and_transform_csv(
            context,
            raw_file_path,
            read_csv_kwargs=treat_config["read_csv_kwargs"],
            reindex_columns=treat_config["reindex_columns"],
            map_columns=treat_config["map"],
            map_values=aux_code_list,
            reorder_columns=treat_config["reorder_columns"]
            if "reorder_columns" in treat_config
            else None,
        ),
    )
    treated_data = fn_divide_columns(
        df=treated_data,
        cols_to_divide=config["divide_columns"] if "divide_columns" in config else None,
    )
    # Save treated file locally
    treated_file_path = fn_save_treated_local(
        context,
        df=treated_data,
        file_path=config["paths"]["file_path"],
        file_type=config["paths"]["file_type"],
    )
    # Upload treated to BigQuery
    fn_upload_to_datalake(
        context,
        table_id=config["paths"]["table_id"],
        dataset_id=config["paths"]["dataset_id"],
        file_paths=[treated_file_path],
        partitions=config["paths"]["partitions"],
        modes=config["upload_to_datalake"]["modes"],
        # is_init=True,  # TODO: não está funcionando a subida de dados aqui!
    )


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
            resource_defs={
                "basedosdados_config": basedosdados_config,
                "timezone_config": timezone_config,
                "discord_webhook": discord_webhook,
            },
        ),
    ],
    tags={
        "pipeline": "br_rj_riodejaneiro_rdo_registros",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "250m", "memory": "500Mi"},
                    "limits": {"cpu": "1500m", "memory": "1Gi"},
                },
            }
        },
    },
)
def br_rj_riodejaneiro_rdo_registros():

    runs = get_runs()
    runs.map(execute_run)
