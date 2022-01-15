# Auxiliar packages
from datetime import datetime, timedelta
from pathlib import Path
import os
import re
import pendulum
import pandas as pd
from dateutil import parser
from basedosdados import Storage, read_table

# Dagster
from dagster import solid, pipeline, ModeDefinition, RetryPolicy
from dagster.experimental import (
    DynamicOutput,
    DynamicOutputDefinition,
)

# Resources
from repositories.libraries.basedosdados.resources import basedosdados_config
from repositories.capturas.resources import (
    keepalive_key,
    ftp_allowed_paths,
    timezone_config,
    discord_webhook,
    schedule_run_date,
)
from repositories.helpers.datetime import (
    convert_datetime_to_unix_time,
    convert_datetime_to_datetime_string,
)
from repositories.helpers.helpers import read_config
from repositories.helpers.hooks import (
    discord_message_on_failure,
    discord_message_on_success,
    # redis_keepalive_on_failure,
    # redis_keepalive_on_succes,
    # log_critical,
)
from repositories.libraries.basedosdados.solids import (
    create_or_append_table,
    cleanup_local,
)
from repositories.helpers.io import connect_ftp


@solid(
    required_resource_keys={"schedule_run_date", "ftp_allowed_paths"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    output_defs=[
        DynamicOutputDefinition(name="file_records"),
    ],
)
def get_file_paths_from_ftp(context):
    """
    Search for files inside previous interval (days) from current date,
    get filename and partitions (from filename) on FTP client.
    """
    ftp_allowed_paths = context.resources.ftp_allowed_paths["values"]
    execution_time = context.resources.schedule_run_date["date"]
    # Define interval for date search
    now = datetime.strptime(execution_time, "%Y-%m-%d") + timedelta(
        hours=11, minutes=30
    )
    min_timestamp = convert_datetime_to_unix_time(
        now - timedelta(days=1)
    )  # Saturday, January 8, 2022 2:30:00 PM
    max_timestamp = convert_datetime_to_unix_time(
        now
    )  # Sunday, January 9, 2022 2:30:00 PM
    context.log.info(f"{execution_time} of type {type(execution_time)}")
    # Connect to FTP & search files
    ftp_client = connect_ftp(
        os.getenv("FTPS_HOST"), os.getenv("FTPS_USERNAME"), os.getenv("FTPS_PWD")
    )
    for folder_name in ftp_allowed_paths:
        files_updated_times = {
            file: datetime.timestamp(parser.parse(info["modify"]))
            for file, info in ftp_client.mlsd(folder_name)
        }
        # Get files modified inside interval
        for filename, file_mtime in files_updated_times.items():
            if file_mtime >= min_timestamp and file_mtime < max_timestamp:
                context.log.info(
                    f"Found file {filename} at folder {folder_name} with timestamp {str(file_mtime)}"
                )
                # Get date from file
                date = re.findall("2\d{3}\d{2}\d{2}", filename)[-1]
                # Get local config for file (yaml)
                config_path = (
                    Path(__file__).parent
                    / f"{folder_name}_{filename.split('_')[0]}.yaml"
                )
                bd_config = read_config(config_path)["resources"][
                    "basedosdados_config"
                ]["config"]
                # Set file infos
                mapping_key = filename.split(".")[0]
                file_info = {
                    "mapping_key": mapping_key,
                    "ftp_path": folder_name + "/" + filename,
                    "partitions": f"ano={date[:4]}/mes={date[4:6]}/dia={date[6:]}",  # "ano=2021/mes=01/dia=07"
                    "table_id": bd_config["table_id"],  # rho5_registros_stpl
                    "config_path": config_path,
                }
                context.log.info(f"Create file info: {file_info}")
                yield DynamicOutput(
                    file_info,
                    mapping_key=mapping_key,
                    output_name="file_records",
                )


@solid(
    required_resource_keys={"basedosdados_config", "timezone_config"},
)
def download_and_save_local_from_ftp(context, file_info):
    """
    Downloads file from FTP and saves to data/raw/<dataset_id>/<table_id>.
    """
    dataset_id = context.resources.basedosdados_config["dataset_id"]
    # Set general local path to save file (modes: raw or staging)
    file_info[
        "local_path"
    ] = f'{os.getcwd()}/{os.getenv("DATA_FOLDER", "data")}/{{mode}}/{dataset_id}/{file_info["table_id"]}/{file_info["partitions"]}/{file_info["mapping_key"]}.{{file_type}}'
    # Get raw data
    file_info["raw_path"] = file_info["local_path"].format(mode="raw", file_type="txt")
    Path(file_info["raw_path"]).parent.mkdir(parents=True, exist_ok=True)
    # Get data from FTP - TODO: create get_raw() error alike
    ftp_client = connect_ftp(
        os.getenv("FTPS_HOST"), os.getenv("FTPS_USERNAME"), os.getenv("FTPS_PWD")
    )
    ftp_client.retrbinary(
        "RETR " + file_info["ftp_path"],
        open(file_info["raw_path"], "wb").write,
    )
    ftp_client.quit()
    # Get timestamp of download time
    file_info["timestamp_captura"] = convert_datetime_to_datetime_string(
        pendulum.now(context.resources.timezone_config["timezone"])
    )
    context.log.info(f"Timestamp captura is {file_info['timestamp_captura']}")
    context.log.info(f"Update file info: {file_info}")
    return file_info


@solid(
    required_resource_keys={"basedosdados_config"},
)
def pre_treatment_br_rj_riodejaneiro_rdo(
    context,
    file_info,
    divide_columns_by: int,
):
    dataset_id = context.resources.basedosdados_config["dataset_id"]
    config = read_config(file_info["config_path"])["solids"][
        "pre_treatment_br_rj_riodejaneiro_rdo"
    ]
    context.log.info(f"Config for ETL: {config}")
    # Load data
    df = pd.read_csv(file_info["raw_path"], header=None, delimiter=";", index_col=False)
    # Set column names for those already in the file
    df.columns = config["reindex_columns"][: len(df.columns)]
    # Treat column "codigo", add empty column if doesn't exist
    if ("codigo" in df.columns) and (file_info["table_id"][-4:] == "stpl"):
        df["codigo"] = df["codigo"].str.extract("(?:VAN)(\d+)").astype(str)
    else:
        df["codigo"] = ""
    # Order columns
    if config["reorder_columns"]:
        ordered = [
            config["reorder_columns"][col]
            if col in config["reorder_columns"].keys()
            else i
            for i, col in enumerate(config["reindex_columns"])
        ]
        df = df[list(config["reindex_columns"][col] for col in ordered)]
    else:
        df = df[config["reindex_columns"]]
    # Add timestamp column
    df["timestamp_captura"] = file_info["timestamp_captura"]
    # Divide columns by value
    if config["divide_columns"]:
        df[config["divide_columns"]] = df[config["divide_columns"]].apply(
            lambda x: x / divide_columns_by, axis=1
        )
    # Save treated data
    file_info["treated_path"] = file_info["local_path"].format(
        mode="staging", file_type="csv"
    )
    Path(file_info["treated_path"]).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_info["treated_path"], index=False)
    context.log.info(f'Saves treated data to: {file_info["treated_path"]}')
    return file_info


# TODO: create unique generic function once in Prefect
@solid(
    required_resource_keys={"basedosdados_config"},
)
def _bq_upload(context, file_info):
    """
    Upload files to BigQuery (BQ).
    * treated file ["treated_path" key] -> creates or append to BQ table
    * raw file ["raw_path" key, optional] -> if exists, uploads to bucket.
    """
    dataset_id = context.resources.basedosdados_config["dataset_id"]
    if file_info["raw_path"]:
        st = Storage(table_id=file_info["table_id"], dataset_id=dataset_id)
        context.log.info(
            f'Uploading {file_info["raw_path"]} to {st.bucket_name}/raw/{dataset_id}/{file_info["table_id"]}'
        )
        st.upload(
            path=file_info["raw_path"],
            partitions=file_info["partitions"],
            mode="raw",
            if_exists="replace",
        )
    # Creates and publish table if it does not exist, append to it otherwise
    if file_info["partitions"]:
        # If table is partitioned, get parent dir where partitions are stored
        path = file_info["treated_path"].split(file_info["partitions"])[0]
    else:
        path = file_info["treated_path"]
    context.log.info("Path: {}".format(path))
    create_or_append_table(
        context,
        dataset_id=dataset_id,
        table_id=file_info["table_id"],
        path=path,
    )
    # Delete local Files
    context.log.info(
        f'Clean up local: {file_info["raw_path"]}, {file_info["treated_path"]}'
    )
    cleanup_local(
        filepath=file_info["treated_path"], raw_filepath=file_info["raw_path"]
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
                "keepalive_key": keepalive_key,
                "ftp_allowed_paths": ftp_allowed_paths,
                "schedule_run_date": schedule_run_date,
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
    # Get ftp file paths & config for each modal
    file_records = get_file_paths_from_ftp()
    # Extract, transform and (up)load data
    file_records.map(
        lambda f: _bq_upload(
            pre_treatment_br_rj_riodejaneiro_rdo(download_and_save_local_from_ftp(f))
        )
    )
    # TODO: upload_logs_to_bq(timestamp, error)
