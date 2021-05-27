import os
from dagster import RunRequest, sensor
from pathlib import Path
from dateutil import parser
import re
import jinja2
from datetime import datetime

from repositories.helpers.helpers import read_config
from repositories.helpers.implicit_ftp import ImplicitFTP_TLS
from repositories.helpers.logging import logger


RDO_DIRECTORY = os.getenv("RDO_DATA", "/opt/dagster/app/data/RDO_DATA")
FTPS_DIRECTORY = os.getenv("FTPS_DATA", "/opt/dagster/app/data/FTPS_DATA")
GTFS_DIRECTORY = os.getenv("GTFS_DATA", "/opt/dagster/app/data/GTFS_DATA")
ALLOWED_FOLDERS = ["SPPO", "STPL"]

def build_run_key(filename, mtime):
    return f"{filename}:{str(mtime)}"


def parse_run_key(run_key):
    parts = run_key.split(":")
    return parts[0], float(parts[1])

def getListOfFiles(dirName):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles

def connect_ftp(HOST, USERNAME, PWD):
    ftp_client = ImplicitFTP_TLS()
    ftp_client.connect(host=HOST, port=990 )
    ftp_client.login(user=USERNAME, passwd=PWD)
    ftp_client.prot_p()
    return ftp_client


@sensor(pipeline_name="br_rj_riodejaneiro_rdo_registros", mode="dev")
def rdo_sensor(context):
    last_mtime = parse_run_key(context.last_run_key)[1] if context.last_run_key else 0

    for filepath in getListOfFiles(RDO_DIRECTORY):
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            _file_name = filepath.split(RDO_DIRECTORY)[1].strip('/')
            file_mtime = fstats.st_mtime
            if file_mtime > last_mtime:
                # the run key should include mtime if we want to kick off new runs based on file modifications
                run_key = build_run_key(filepath, file_mtime)

                # Parse mode, dataset_id and table_id
                path_list = _file_name.split('/')
                dataset_id = path_list[1]
                table_id = path_list[2]
                filename = path_list[-1].split(".")[0]

                config = read_config(Path(__file__).parent / f'{dataset_id}/{table_id}.yaml') 
                config['solids']['parse_file_path_and_partitions']['inputs']['bucket_path']['value'] = _file_name
                config['solids']['upload_file_to_storage'] = {"inputs": {"file_path": {"value": filepath}}}
                config['resources']['basedosdados_config'] = {"config": {"dataset_id": dataset_id,
                                                                         "table_id": table_id}}
                yield RunRequest(run_key=run_key, run_config=config)


@sensor(pipeline_name="br_rj_riodejaneiro_gtfs_planned_feed", mode="dev")
def gtfs_sensor(context):
    last_mtime = parse_run_key(context.last_run_key)[1] if context.last_run_key else 0

    for filepath in getListOfFiles(GTFS_DIRECTORY):
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            _file_name = filepath.split(GTFS_DIRECTORY)[1].strip('/')
            file_mtime = fstats.st_mtime
            if file_mtime > last_mtime:
                # the run key should include mtime if we want to kick off new runs based on file modifications
                run_key = build_run_key(filepath, file_mtime)

                # Parse mode, dataset_id and table_id
                path_list = _file_name.split('/')
                dataset_id = path_list[0]
                table_id = path_list[1]
                # filename = path_list[-1].split(".")[0]

                config = read_config(Path(__file__).parent / f'{dataset_id}/{table_id}.yaml') 
                config['solids']['create_gtfs_version_partition'] = {'inputs': {'original_filepath': {
                    'value': filepath}}}
                config['solids']['get_gtfs_files'] = {'inputs': {'original_filepath': {
                    'value': filepath}}}
                config['solids']['open_gtfs_feed'] = {'inputs': {'original_filepath': {
                    'value': filepath}}}
                config['solids']['upload_file_to_storage'] = {'inputs': {'file_path': {
                    'value': filepath}}}
                config['solids']['get_realized_trips'] = {'inputs': {'file_path': {
                    'value': filepath}}}
                config['resources']['basedosdados_config'] = {"config": {"dataset_id": dataset_id,
                                                                         "table_id": table_id}}
                yield RunRequest(run_key=run_key, run_config=config)

@sensor(pipeline_name="br_rj_riodejaneiro_rdo_registros", mode="dev")
def ftps_sensor(context):
    last_mtime = float(context.cursor) if context.cursor else 0
    logger.debug(f"Current cursor: {last_mtime}")
    max_mtime = last_mtime + 259200
    next_mtime = max_mtime
    ftp_client = connect_ftp(os.getenv("FTPS_HOST"), os.getenv("FTPS_USERNAME"), os.getenv("FTPS_PWD"))

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
                        config = read_config(Path(__file__).parent / f'br_rj_riodejaneiro_rdo/{folder_name}_{fileprefix}.yaml') 
                        table_id = config['resources']['basedosdados_config']['config']['table_id']
                        date = tuple(re.findall("\d+", filename))
                        ano = date[2][:4]
                        mes = date[2][4:6]
                        dia = date[2][6:]
                        relative_filepath = Path('raw/br_rj_riodejaneiro_rdo', table_id, f'ano={ano}', f'mes={mes}', f'dia={dia}')
                        local_filepath = Path(FTPS_DIRECTORY, relative_filepath)
                        Path(local_filepath).mkdir(parents=True, exist_ok=True)
                        with open(f'{local_filepath}/{filename}', 'wb') as local_file:
                            ftp_client.retrbinary('RETR ' + f'{folder_name}/{filename}', local_file.write)
                        
                        # Run pipeline
                        config['solids']['parse_file_path_and_partitions']['inputs']['bucket_path']['value'] = f'{relative_filepath}/{filename}'
                        config['solids']['upload_file_to_storage'] = {"inputs": {"file_path": {"value": f'{local_filepath}/{filename}'}}}
                        yield RunRequest(run_key=run_key, run_config=config)

                    except jinja2.TemplateNotFound as err:
                        logger.warning(f"Config file for file {filename} was not found. Skipping file.")
                    
                next_mtime = min(file_mtime, max_mtime)
                ftp_client.cwd('/')
        else:
            logger.warning(f"Skipping file {folder[0]} since it is not inside a folder")
            continue

        logger.debug(f"Setting cursor to {next_mtime}")
        context.update_cursor(str(next_mtime))