import os
import re
from pathlib import Path
from datetime import datetime, timedelta

import jinja2
import pendulum
import pandas as pd
import basedosdados as bd
from dateutil import parser
from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    PresetDefinition,
)
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

#
# Previous solids, now functions
#
def fn_download_file_from_ftp(ftp_path: str, local_path: str) -> None:
    """Downloads a file from FTP to the local storage"""
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    ftp_client = connect_ftp(
        os.getenv("FTPS_HOST"), os.getenv("FTPS_USERNAME"), os.getenv("FTPS_PWD")
    )
    ftp_client.retrbinary("RETR " + ftp_path, open(local_path, "wb").write)
    ftp_client.quit()


def fn_parse_file_path_and_partitions(context, bucket_path):

    # Parse bucket to get mode, dataset_id, table_id and filename
    path_list = bucket_path.split("/")
    dataset_id = path_list[1]
    table_id = path_list[2]
    filename = path_list[-1].split(".")[0]
    filetype = path_list[-1].split(".")[1]

    # Parse bucket to get partitions
    partitions = re.findall("\/([^\/]*?)=(.*?)(?=\/)", bucket_path)
    partitions = "/".join(["=".join([field for field in item]) for item in partitions])

    # Get data folder from environment variable
    data_folder = os.getenv("DATA_FOLDER", "data")

    folder = f"{os.getcwd()}/{data_folder}/{{mode}}/{dataset_id}/{table_id}/"
    file_path = f"{folder}/{partitions}/{filename}.{{filetype}}"
    context.log.info(f"creating file path {file_path}")

    return filename, filetype, file_path, partitions


def fn_upload_file_to_storage(
    context, file_path, partitions=None, mode="raw", table_id=None
):

    # Upload to storage
    # If not specific table_id, use resource one
    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    st = bd.Storage(table_id=table_id, dataset_id=dataset_id)

    context.log.debug(
        f"Uploading file {file_path} to mode {mode} with partitions {partitions}"
    )
    st.upload(path=file_path, mode=mode, partitions=partitions, if_exists="replace")

    return True


def fn_get_file_from_storage(
    context,
    file_path,
    filename,
    partitions,
    mode="raw",
    filetype="xlsx",
    uploaded=True,
    table_id=None,
):

    # Download from storage
    # If not specific table_id, use resource one
    if not table_id:
        table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    _file_path = file_path.format(mode=mode, filetype=filetype)

    st = StoragePlus(table_id=table_id, dataset_id=dataset_id)
    context.log.debug(f"File path: {_file_path}")
    context.log.debug(f"filename: {filename}")
    context.log.debug(f"partition: {partitions}")
    context.log.debug(f"mode: {mode}")
    st.download(
        filename=filename + "." + filetype,
        file_path=_file_path,
        partitions=partitions,
        mode=mode,
        if_exists="replace",
    )
    return _file_path


def fn_load_and_reindex_csv(
    file_path: str, read_csv_kwargs: dict, reindex_kwargs: dict
):
    # Rearrange columns
    # df = pd.read_csv(file_path, delimiter=delimiter,
    #                  skiprows=header_lines,
    #                  names=original_header,
    #                  index_col=False)
    df = pd.read_csv(file_path, **read_csv_kwargs)
    df = df.reindex(**reindex_kwargs)
    return df


def fn_add_timestamp(context, df):
    timezone = context.resources.timezone_config["timezone"]
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    context.log.debug(", ".join(list(df.columns)))

    return df


def fn_divide_columns(df, cols_to_divide=None, value=100):
    if cols_to_divide:
        # Divide columns by value
        df[cols_to_divide] = df[cols_to_divide].apply(lambda x: x / value, axis=1)
    return df


def fn_upload_to_bigquery(
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


def fn_save_treated_local(df, file_path, mode="staging"):

    _file_path = file_path.format(mode=mode, filetype="csv")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(_file_path, index=False)

    return _file_path


@solid(output_defs=[DynamicOutputDefinition(dict)])
def get_runs(context, execution_date):
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    now = execution_date + timedelta(hours=11, minutes=30)
    this_time_yesterday = now - timedelta(days=1)
    min_timestamp = convert_datetime_to_unix_time(this_time_yesterday)
    max_timestamp = convert_datetime_to_unix_time(now)
    context.log.info(f"{execution_date} of type {type(execution_date)}")
    ftp_client = connect_ftp(
        os.getenv("FTPS_HOST"), os.getenv("FTPS_USERNAME"), os.getenv("FTPS_PWD")
    )

    # Change to working directory
    ftp_client.cwd("/")
    for folder in ftp_client.mlsd():

        # Config yaml file will be folder_fileprefix.yaml
        if folder[1]["type"] == "dir" and folder[0] in ALLOWED_FOLDERS:
            # CWD to folder
            context.log.info(f"Entering folder {folder[0]}")
            folder_name = folder[0].lower()

            # Read file list
            for filepath in ftp_client.mlsd(folder_name):
                filename = filepath[0]
                fileprefix = filename.split("_")[0].lower()
                timestamp = filepath[1]["modify"]
                file_mtime = datetime.timestamp(parser.parse(timestamp))

                if file_mtime >= min_timestamp and file_mtime < max_timestamp:

                    # Download file to local folder
                    try:
                        config = read_config(
                            Path(__file__).parent / f"{folder_name}_{fileprefix}.yaml"
                        )
                        table_id = config["resources"]["basedosdados_config"]["config"][
                            "table_id"
                        ]
                        date = tuple(re.findall("\d+", filename))
                        ano = date[2][:4]
                        mes = date[2][4:6]
                        dia = date[2][6:]
                        relative_filepath = Path(
                            "raw/br_rj_riodejaneiro_rdo",
                            table_id,
                            f"ano={ano}",
                            f"mes={mes}",
                            f"dia={dia}",
                        )
                        local_filepath = Path(FTPS_DIRECTORY, relative_filepath)
                        Path(local_filepath).mkdir(parents=True, exist_ok=True)

                        ftp_path = str(Path(folder_name, filename))
                        local_path = str(Path(local_filepath, filename))

                        # Run pipeline
                        config["solids"]["download_file_from_ftp"]["inputs"] = {
                            "ftp_path": {"value": ftp_path},
                            "local_path": {"value": local_path},
                        }
                        config["solids"]["parse_file_path_and_partitions"]["inputs"][
                            "bucket_path"
                        ]["value"] = f"{relative_filepath}/{filename}"
                        config["solids"]["upload_file_to_storage"] = {
                            "inputs": {"file_path": {"value": local_path}}
                        }
                        yield DynamicOutput(
                            config, mapping_key=f"{folder_name}_{fileprefix}"
                        )

                    except jinja2.TemplateNotFound as err:
                        context.log.warning(
                            f"Config file for file {filename} was not found. Skipping file."
                        )
                        context.log.warning(f"{Path(__file__).parent}")
                ftp_client.cwd("/")
        else:
            context.log.warning(
                f"Skipping file {folder[0]} since it is not inside a folder"
            )
            continue


@solid(required_resource_keys={"timezone_config", "basedosdados_config"},)
def execute_run(context, run_config: dict):

    # Get file from FTPS and save it locally
    ftp_path = run_config["solids"]["download_file_from_ftp"]["inputs"]["ftp_path"][
        "value"
    ]
    local_path = run_config["solids"]["download_file_from_ftp"]["inputs"]["local_path"][
        "value"
    ]
    fn_download_file_from_ftp(ftp_path, local_path)

    # Parse file path and get partitions
    bucket_path = run_config["solids"]["parse_file_path_and_partitions"]["inputs"][
        "bucket_path"
    ]["value"]
    filename, filetype, file_path, partitions = fn_parse_file_path_and_partitions(
        context, bucket_path
    )

    # Upload file to GCS
    uploaded = fn_upload_file_to_storage(context, local_path, partitions)

    # Get file from GCS
    raw_file_path = fn_get_file_from_storage(
        context,
        file_path=file_path,
        filename=filename,
        partitions=partitions,
        filetype=filetype,
        uploaded=uploaded,
    )

    # Extract, load and transform
    try:
        read_csv_kwargs = run_config["solids"]["load_and_reindex_csv"]["config"][
            "read_csv_kwargs"
        ]
    except:
        context.log.warning(
            f"Error reading read_csv_kwargs config file for {filename} in folder {file_path}. Skipping file."
        )
        read_csv_kwargs = None
    try:
        reindex_kwargs = run_config["solids"]["load_and_reindex_csv"]["config"][
            "reindex_kwargs"
        ]
    except:
        context.log.warning(
            f"Error reading reindex_kwargs config file for {filename} in folder {file_path}. Skipping file."
        )
        reindex_kwargs = None
    treated_data = fn_load_and_reindex_csv(
        raw_file_path, read_csv_kwargs=read_csv_kwargs, reindex_kwargs=reindex_kwargs
    )
    treated_data = fn_add_timestamp(context, treated_data)
    try:
        cols_to_divide = run_config["solids"]["divide_columns"]["inputs"][
            "cols_to_divide"
        ]["value"]
    except:
        context.log.warning(
            f"No cols_to_divide was found in config file. Using default cols_to_divide."
        )
        cols_to_divide = None
    treated_data = fn_divide_columns(treated_data, cols_to_divide)

    # Save treated file locally
    treated_file_path = fn_save_treated_local(treated_data, file_path)

    # Upload treated to BigQuery
    modes = run_config["solids"]["upload_to_bigquery"]["inputs"]["modes"]["value"]
    fn_upload_to_bigquery(context, [treated_file_path], partitions, modes=modes)


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
)
def br_rj_riodejaneiro_rdo_registros():

    runs = get_runs()
    runs.map(execute_run)
