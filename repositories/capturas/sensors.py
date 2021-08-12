import os
from dagster import RunRequest, sensor
from pathlib import Path
from dagster.core.definitions.sensor import SensorExecutionContext
from dateutil import parser
import re
import time
import jinja2
from datetime import datetime

from repositories.helpers.helpers import read_config
from repositories.helpers.logging import logger
from repositories.helpers.io import get_list_of_files, build_run_key, parse_run_key, connect_ftp, get_list_of_blobs, filter_blobs_by_modification_time

FTPS_DIRECTORY = os.getenv("FTPS_DATA", "/opt/dagster/app/data/FTPS_DATA")
SENSOR_BUCKET = os.getenv("SENSOR_BUCKET", "rj-smtr-dev")
GTFS_BLOBS_PREFIX = os.getenv("GTFS_BLOBS_PREFIX", "gtfs/")
GTFS_DIRECTORY = os.getenv("GTFS_DATA", "/opt/dagster/app/data/GTFS_DATA")
ALLOWED_FOLDERS = ["SPPO", "STPL"]


@sensor(pipeline_name="br_rj_riodejaneiro_gtfs_planned_feed", mode="dev")
def gtfs_sensor(context):
    last_mtime = parse_run_key(context.last_run_key)[
        1] if context.last_run_key else 0

    for blob in filter_blobs_by_modification_time(
        get_list_of_blobs(GTFS_BLOBS_PREFIX, SENSOR_BUCKET, mode="staging"), last_mtime, after=True,
    ):
        run_key: str = build_run_key(
            blob.name, time.mktime(blob.updated.timetuple()))

        # Parse dataset_id and table_id
        path_list: list = [n for n in blob.name.split(
            GTFS_BLOBS_PREFIX)[1].split("/") if n != ""]
        dataset_id: str = path_list[0]
        table_id: str = path_list[1]

        # Set run configs
        config: dict = read_config(
            Path(__file__).parent / f'{dataset_id}/{table_id}.yaml')
        config['solids']['save_blob_to_tempfile'] = {'inputs': {'blob_path': {
            'value': blob.name}, 'bucket_name': {'value': SENSOR_BUCKET}}}
        config['solids']['create_gtfs_version_partition'] = {'inputs': {'original_filepath': {
            'value': blob.name}, 'bucket_name': {'value': SENSOR_BUCKET}}}
        config['solids']['upload_blob_to_storage'] = {'inputs': {'blob_path': {
            'value': blob.name}, 'bucket_name': {'value': SENSOR_BUCKET}}}
        config['resources']['basedosdados_config'] = {
            "config": {"dataset_id": dataset_id, "table_id": table_id}}

        yield RunRequest(run_key=run_key, run_config=config)


@sensor(pipeline_name="br_rj_riodejaneiro_rdo_registros", mode="dev")
def ftps_sensor(context):
    last_mtime = float(context.cursor) if context.cursor else 0
    logger.debug(f"Current cursor: {last_mtime}")
    max_mtime = last_mtime + 259200
    next_mtime = max_mtime
    ftp_client = connect_ftp(os.getenv("FTPS_HOST"), os.getenv(
        "FTPS_USERNAME"), os.getenv("FTPS_PWD"))

    # Change to working directory
    ftp_client.cwd('/')
    for folder in ftp_client.mlsd():

        # Config yaml file will be folder_fileprefix.yaml
        if folder[1]['type'] == 'dir' and folder[0] in ALLOWED_FOLDERS:
            # CWD to folder
            logger.info(f"Entering folder {folder[0]}")
            folder_name = folder[0].lower()

            # Read file list
            for filepath in ftp_client.mlsd(folder_name):
                filename = filepath[0]
                fileprefix = filename.split('_')[0].lower()
                timestamp = filepath[1]['modify']
                file_mtime = datetime.timestamp(parser.parse(timestamp))

                if file_mtime < max_mtime and file_mtime > last_mtime:
                    # the run key should include mtime if we want to kick off new runs based on file modifications
                    run_key = build_run_key(filename, file_mtime)

                    # Download file to local folder
                    try:
                        config = read_config(Path(
                            __file__).parent / f'br_rj_riodejaneiro_rdo/{folder_name}_{fileprefix}.yaml')
                        table_id = config['resources']['basedosdados_config']['config']['table_id']
                        date = tuple(re.findall("\d+", filename))
                        ano = date[2][:4]
                        mes = date[2][4:6]
                        dia = date[2][6:]
                        relative_filepath = Path(
                            'raw/br_rj_riodejaneiro_rdo', table_id, f'ano={ano}', f'mes={mes}', f'dia={dia}')
                        local_filepath = Path(
                            FTPS_DIRECTORY, relative_filepath)
                        Path(local_filepath).mkdir(parents=True, exist_ok=True)

                        ftp_path = str(Path(folder_name, filename))
                        local_path = str(Path(local_filepath, filename))

                        # Run pipeline
                        config['solids']['download_file_from_ftp']['inputs'] = {
                            'ftp_path': {'value': ftp_path}, 'local_path': {'value': local_path}}
                        config['solids']['parse_file_path_and_partitions']['inputs'][
                            'bucket_path']['value'] = f'{relative_filepath}/{filename}'
                        config['solids']['upload_file_to_storage'] = {"inputs": {
                            "file_path": {"value": local_path}}}
                        yield RunRequest(run_key=run_key, run_config=config)

                    except jinja2.TemplateNotFound as err:
                        logger.warning(
                            f"Config file for file {filename} was not found. Skipping file.")

                next_mtime = min(file_mtime, max_mtime)
                ftp_client.cwd('/')
        else:
            logger.warning(
                f"Skipping file {folder[0]} since it is not inside a folder")
            continue

        logger.debug(f"Setting cursor to {next_mtime}")
        context.update_cursor(str(next_mtime))
