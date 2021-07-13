import os
import time
from pathlib import Path
from datetime import datetime

from google.oauth2 import service_account
from google.cloud import storage
from google.cloud.storage.blob import Blob

from repositories.helpers.implicit_ftp import ImplicitFTP_TLS


def get_list_of_blobs(prefix: str, bucket_name: str) -> list:
    """Gets list of blobs from `bucket_name` with `prefix`, which can be a path"""
    credentials = service_account.Credentials.from_service_account_file(
        Path.home() / ".basedosdados/credentials/prod.json")
    client = storage.Client(credentials=credentials)
    l: list = client.list_blobs(bucket_name, prefix=prefix)
    l = [blob for blob in l if not blob.name.endswith("/")]
    return l


def get_blob(name: str, bucket_name: str) -> Blob:
    """Gets a single blob from `bucket_name`"""
    credentials = service_account.Credentials.from_service_account_file(
        Path.home() / ".basedosdados/credentials/prod.json")
    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket(bucket_name)
    return bucket.get_blob(name)


def filter_blobs_by_modification_time(l: list, ref: float, after: bool = True):
    """Filters blobs by modification time.
    - `after` == True -> blob.updated >= ref
    - `after` == False -> blob.updated < ref"""
    return [blob for blob in l if not((time.mktime(blob.updated.timetuple()) >= ref) != after)]


def get_list_of_files(dirName):
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
            allFiles = allFiles + get_list_of_files(fullPath)
        # Skip .keep files
        elif not fullPath.endswith(".keep"):
            allFiles.append(fullPath)
    return allFiles


def build_run_key(filename, mtime):
    return f"{filename}:{str(mtime)}"


def parse_run_key(run_key: str):
    parts = run_key.split(":")
    return parts[0], float(parts[1])


def connect_ftp(HOST, USERNAME, PWD):
    ftp_client = ImplicitFTP_TLS()
    ftp_client.connect(host=HOST, port=990)
    ftp_client.login(user=USERNAME, passwd=PWD)
    ftp_client.prot_p()
    return ftp_client


def parse_filepath_to_tablename(filepath: str) -> str:
    """ Parses a file path to a table name on BigQuery.
    Disclaimer: do not use dots on the table name.

    Example:
        - File path: "./dashboard_monitoramento_brt/registros_filtrada.sql"
        - Expected output: "rj-smtr.dashboard_monitoramento_brt.registros_filtrada"

    Arguments:
        - filepath: str -> the path to the file you wanna parse
    """
    # Gets project name from env
    prefix: str = os.getenv("BQ_PROJECT_NAME", "rj-smtr-dev")

    # Split file path into parts
    spl: list = filepath.split("/")

    # Assert length matches minimum required (dataset name + table name)
    if len(spl) < 2:
        raise ValueError(
            "Can't parse file path {} to BigQuery table name. Reason: path too short!".format(filepath))

    # Extracts dataset and table names
    dataset_name: str = spl[-2]
    table_name: str = spl[-1]

    # Removes table name file terminations (.sql, .py, etc.)
    table_name = table_name.split(".")[0]

    # Returns full BQ name
    return ".".join([prefix, dataset_name, table_name])
