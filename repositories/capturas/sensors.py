import os
from dagster import RunRequest, sensor
from pathlib import Path
from repositories.helpers.helpers import read_config


RDO_DIRECTORY = os.getenv("RDO_DATA", "/opt/dagster/app/data/RDO_DATA")
GTFS_DIRECTORY = os.getenv("GTFS_DATA", "/opt/dagster/app/data/GTFS_DATA")

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