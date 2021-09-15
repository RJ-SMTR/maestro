import os
import time
import json
import yaml
import base64
import datetime
import requests
from pathlib import Path

import pytz
import jinja2
from dagster import SolidExecutionContext
from google.oauth2 import service_account
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from google.cloud.storage.blob import Blob
from google.cloud.bigquery.table import RowIterator
from google.api_core.exceptions import Conflict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from repositories.helpers.constants import constants
from repositories.helpers.implicit_ftp import ImplicitFTP_TLS
from repositories.helpers.datetime import convert_datetime_to_datetime_string


def get_bigquery_client() -> bigquery.Client:
    """Returns a BigQuery client"""
    credentials = get_credentials_from_env()
    return bigquery.Client(project=os.getenv("BQ_PROJECT_NAME"), credentials=credentials)


def run_query(query: str, timeout: float = None):
    """Runs a query on BigQuery"""
    client = get_bigquery_client()
    return client.query(query, timeout=timeout).result()


def insert_results_to_table(row_iterator: RowIterator, table_name: str) -> list:
    """Inserts a row iterator into a table"""
    client = get_bigquery_client()
    table = client.get_table(table_name)
    errors = client.insert_rows(table, row_iterator)
    return errors


def check_if_table_exists(table_name: str):
    """Checks if a table exists in BigQuery"""
    client = get_bigquery_client()
    try:
        client.get_table(table_name)
        return True
    except NotFound:
        return False


def get_table_type(table_name: str):
    """Returns the type of a table in BigQuery"""
    client = get_bigquery_client()
    table = client.get_table(table_name)
    return table.table_type


def get_session_builder() -> sessionmaker:
    """Returns a session builder for the SQLAlchemy engine"""
    db_uri = f'postgresql://{os.getenv("DAGSTER_POSTGRES_USER")}:{os.getenv("DAGSTER_POSTGRES_PASSWORD")}@{os.getenv("DAGSTER_POSTGRES_HOST")}/{os.getenv("DAGSTER_POSTGRES_DB")}'
    engine = create_engine(db_uri, echo=False)
    return sessionmaker(bind=engine)


def get_credentials_from_env(mode: str = "prod") -> service_account.Credentials:
    """Gets credentials from env vars"""
    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    env: str = os.getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if env == "":
        raise ValueError(
            f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!")
    info: dict = json.loads(base64.b64decode(env))
    return service_account.Credentials.from_service_account_info(info)


def get_list_of_blobs(prefix: str, bucket_name: str, mode: str = "prod") -> list:
    """Gets list of blobs from `bucket_name` with `prefix`, which can be a path"""
    credentials = get_credentials_from_env(mode=mode)
    client = storage.Client(credentials=credentials)
    l: list = client.list_blobs(bucket_name, prefix=prefix)
    l = [blob for blob in l if not blob.name.endswith("/")]
    return l


def get_blob(name: str, bucket_name: str, mode: str = "prod") -> Blob:
    """Gets a single blob from `bucket_name`"""
    credentials = get_credentials_from_env(mode=mode)
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


def fetch_branch_sha(github_repo_name: str, branch_name: str):
    """Fetches the SHA of a branch from Github"""
    url = f"https://api.github.com/repos/{github_repo_name}/git/refs"
    response = requests.get(url)
    if response.status_code != 200:
        return None
    else:
        branches = response.json()
        for branch in branches:
            if branch["ref"] == f"refs/heads/{branch_name}":
                return branch["object"]["sha"]
    return None


def update_view(table_name: str, defaults_dict: dict, dataset_name: str, view_name: str, view_yaml: str, delete: bool = False, context: SolidExecutionContext = None):

    from repositories.queries.sensors import SENSOR_BUCKET
    from repositories.queries.sensors import MATERIALIZED_VIEWS_PREFIX

    # Table ID can't be empty
    if table_name is None or table_name == "":
        raise Exception("Table name can't be None or empty!")

    # Setup credentials and BQ client
    credentials = get_credentials_from_env()
    client = bigquery.Client(credentials=credentials)

    # Delete
    if (delete):
        client.delete_table(table_name, not_found_ok=True)

    # Create/update
    else:
        # Load configs from GCS
        view_blob = get_blob(
            view_yaml, SENSOR_BUCKET, mode="staging")
        if view_blob:
            view_dict = yaml.safe_load(
                view_blob.download_as_string())
        else:
            view_dict = {}

        # Merge configs
        query_params = {**defaults_dict, **view_dict}

        # Build base configs
        now = datetime.datetime.now(
            pytz.timezone("America/Sao_Paulo"))
        with open(str(Path(__file__).parent.parent / "queries/materialized_views_base_config.yaml"), "r") as f:
            base_params: dict = yaml.safe_load(f)
        base_params["run_timestamp"] = "'{}'".format(
            convert_datetime_to_datetime_string(now))
        base_params["maestro_sha"] = "'{}'".format(fetch_branch_sha(
            constants.MAESTRO_REPOSITORY.value, constants.MAESTRO_DEFAULT_BRANCH.value))
        base_params["maestro_bq_sha"] = "'{}'".format(fetch_branch_sha(
            constants.MAESTRO_BQ_REPOSITORY.value, constants.MAESTRO_BQ_DEFAULT_BRANCH.value))

        # Few more params
        custom_params = {
            "date_range_start": "'{}'".format(query_params["backfill"]["start_timestamp"]),
            "date_range_end": "'{}'".format(convert_datetime_to_datetime_string(now))
        }  # Backfill parameters

        # Get query on GCS
        query_file = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, dataset_name, view_name)}.sql'
        if context:
            context.log.info(f"Fetching query from GCS: {query_file}")
        query_blob = get_blob(
            query_file, SENSOR_BUCKET, mode="staging")
        base_query = query_blob.download_as_string().decode("utf-8")

        # Build query for view
        template = jinja2.Template(base_query)
        query = template.render(
            **base_params, **query_params["parameters"], **custom_params)

        # SQL can't be empty if not removing table
        if (query is None or query == ""):
            raise Exception("Query can't be None or empty!")

        table = bigquery.Table(table_name)
        table.view_query = query

        # Always Overwrite
        try:
            client.create_table(table)
        except Conflict:
            client.delete_table(table)
            client.create_table(table)
