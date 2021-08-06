import os
import time
import yaml
import datetime
from pathlib import Path
from google.cloud.storage.blob import Blob

from redis_pal import RedisPal
from dagster import RunRequest, sensor, SensorExecutionContext

from repositories.helpers.helpers import read_config
from repositories.helpers.constants import constants
from repositories.helpers.datetime import determine_whether_to_execute_or_not, convert_datetime_to_datetime_string
from repositories.helpers.io import (
    build_run_key,
    get_blob,
    get_list_of_blobs,
    parse_filepath_to_tablename,
    parse_run_key,
    fetch_branch_sha,
)

SENSOR_BUCKET = os.getenv("SENSOR_BUCKET", "rj-smtr-dev")
VIEWS_PREFIX = os.getenv("VIEWS_PREFIX", "queries/views/")
MATERIALIZED_VIEWS_PREFIX = os.getenv(
    "MATERIALIZED_VIEWS_PREFIX", "queries/materialized_views/")


@sensor(pipeline_name="update_view_on_bigquery", mode="dev")
def views_sensor(context: SensorExecutionContext):
    """Sensor for updating views on file changes.

    For every new or modified file, the pipeline `update_view_on_bigquery`
    is triggered. This ensures BQ views are always up-to-date.
    """
    # Start RedisPal (we use that to check on deleted files)
    rp: RedisPal = RedisPal(host=constants.REDIS_HOST.value)

    # Get last modification time
    last_mtime = parse_run_key(context.last_run_key)[
        1] if context.last_run_key else 0

    # Get list of files
    list_of_blobs: list = get_list_of_blobs("queries/views", SENSOR_BUCKET)

    # Get previous set of files from Redis
    prev_set_of_blobs: set = rp.get("queries_files_set")

    # Parse current list of files to set
    set_of_blobs: set = set([blob.name for blob in list_of_blobs])

    # If we've cached a previous list of files
    if prev_set_of_blobs is not None:

        # Get deleted files
        deleted: set = prev_set_of_blobs - set_of_blobs

        # For every deleted file, delete corresponding view
        blob_name: str = None
        for blob_name in deleted:
            if blob_name.endswith(".sql"):

                # Extract table name from file path
                table_name: str = parse_filepath_to_tablename(
                    "/".join([n for n in blob_name.split(VIEWS_PREFIX)[1].split("/") if n != ""]))

                # Set a run key so we can keep track of changes
                run_key: str = build_run_key("delete-" + blob_name, last_mtime)

                # Load run configuration
                config: dict = read_config(
                    Path(__file__).parent / "views.yaml")

                # Set inputs
                config["solids"]["update_view"]["inputs"]["view_sql"]["value"] = ""
                config["solids"]["update_view"]["inputs"]["table_name"]["value"] = table_name
                config["solids"]["update_view"]["inputs"]["delete"]["value"] = True

                # Yield a run request
                yield RunRequest(run_key=run_key, run_config=config)

    # Cache current file list
    rp.set("queries_files_set", set_of_blobs)

    # Iterate over all SQL files
    blob: Blob = None
    for blob in list_of_blobs:
        if blob.name.endswith(".sql"):

            # Get file's last modification timestamp
            file_mtime = time.mktime(blob.updated.timetuple())

            # If file has modified
            if file_mtime > last_mtime:

                # Extract table name from file path
                table_name: str = parse_filepath_to_tablename(
                    "/".join([n for n in blob.name.split(VIEWS_PREFIX)[1].split("/") if n != ""]))

                # Extract query from file
                query: str = blob.download_as_string().decode("utf-8")

                # Set a run key so we can keep track of changes
                run_key: str = build_run_key(blob.name, file_mtime)

                # Load run configuration
                config: dict = read_config(
                    Path(__file__).parent / "views.yaml")

                # Set inputs
                config["solids"]["update_view"]["inputs"]["view_sql"]["value"] = query
                config["solids"]["update_view"]["inputs"]["table_name"]["value"] = table_name
                config["solids"]["update_view"]["inputs"]["delete"]["value"] = False

                # Yield a run request
                yield RunRequest(run_key=run_key, run_config=config)


@sensor(pipeline_name="update_managed_materialized_views", mode="dev")
def materialized_views_update_sensor(context: SensorExecutionContext):
    """Sensor for updating materialized views on file changes.

    For every new or modified file, the pipeline `update_managed_materialized_views`
    is triggered. This ensures BQ materialized views are always up-to-date.
    """
    # Start RedisPal (we use that to check on deleted files)
    rp: RedisPal = RedisPal(host=constants.REDIS_HOST.value)

    # Get last modification time
    last_mtime = parse_run_key(context.last_run_key)[
        1] if context.last_run_key else 0

    # Get list of files
    list_of_blobs: list = get_list_of_blobs(
        MATERIALIZED_VIEWS_PREFIX, SENSOR_BUCKET)

    # Get previous set of files from Redis
    prev_set_of_blobs: set = rp.get("materialized_views_files_set")

    # Parse current list of files to set
    set_of_blobs: set = set([blob.name for blob in list_of_blobs])

    # If we've cached a previous list of files
    if prev_set_of_blobs is not None:

        # Get deleted files
        deleted: set = prev_set_of_blobs - set_of_blobs

        # For every deleted file, delete corresponding view
        blob_name: str = None
        for blob_name in deleted:
            if blob_name.endswith(".sql"):

                # Set a run key so we can keep track of changes
                run_key: str = build_run_key(
                    "delete-materialized-" + blob_name, last_mtime)

                # Load run configuration
                config: dict = read_config(
                    Path(__file__).parent / "materialized_views_update.yaml")

                # Set inputs
                config["solids"]["update_materialized_view_on_redis"]["inputs"]["blob_name"]["value"] = blob_name
                config["solids"]["update_materialized_view_on_redis"]["inputs"]["cron_expression"]["value"] = ""
                config["solids"]["update_materialized_view_on_redis"]["inputs"]["delete"]["value"] = True

                # Yield a run request
                yield RunRequest(run_key=run_key, run_config=config)

    # Cache current file list
    rp.set("materialized_views_files_set", set_of_blobs)

    # Iterate over all YAML files
    blob: Blob = None
    for blob in list_of_blobs:
        if blob.name.endswith(".yaml") or blob.name.endswith(".yml"):

            # Get file's last modification timestamp
            file_mtime = time.mktime(blob.updated.timetuple())

            # If file has modified
            if file_mtime > last_mtime:

                # Extract query from file
                materialized_view_config: dict = yaml.safe_load(
                    blob.download_as_string().decode("utf-8"))

                # Set a run key so we can keep track of changes
                run_key: str = build_run_key(blob.name, file_mtime)

                # Check if query has also changed
                query_blob_name = blob.name.split(".y")[0] + ".sql"
                query_blob = get_blob(query_blob_name, SENSOR_BUCKET)
                if query_blob is not None and time.mktime(query_blob.updated.timetuple()) > last_mtime:
                    query_modified = True
                else:
                    query_modified = False

                # Load run configuration
                config: dict = read_config(
                    Path(__file__).parent / "materialized_views_update.yaml")

                # Set inputs
                config["solids"]["update_materialized_view_on_redis"]["inputs"]["blob_name"]["value"] = blob.name
                config["solids"]["update_materialized_view_on_redis"]["inputs"][
                    "cron_expression"]["value"] = materialized_view_config["scheduling"]["cron"]
                config["solids"]["update_materialized_view_on_redis"]["inputs"]["delete"]["value"] = False
                config["solids"]["update_materialized_view_on_redis"]["inputs"]["query_modified"]["value"] = query_modified

                # Yield a run request
                yield RunRequest(run_key=run_key, run_config=config)


@sensor(pipeline_name="materialize_view", mode="dev")
def materialized_views_execute_sensor(context: SensorExecutionContext):
    """Sensor for executing materialized views based on cron expressions."""
    # Start RedisPal (we use that to check cron expressions)
    rp: RedisPal = RedisPal(host=constants.REDIS_HOST.value)

    # Get managed materialized views
    managed_materialized_views: dict = rp.get("managed_materialized_views")

    # Get current timestamp
    now = datetime.datetime.now()

    # Iterate over all managed materialized views
    for blob_name, view_config in managed_materialized_views.items():
        if view_config["last_run"] is None or determine_whether_to_execute_or_not(view_config["cron_expression"], now, view_config["last_run"]):

            # Load run configuration
            config: dict = read_config(
                Path(__file__).parent / "materialized_views_execute.yaml")

            # Get query
            blob: Blob = get_blob(blob_name.split(
                ".y")[0] + ".sql", SENSOR_BUCKET)
            query: str = blob.download_as_string().decode("utf-8")

            # Extract table name from blob path
            table_name: str = parse_filepath_to_tablename(
                "/".join([n for n in blob.name.split(MATERIALIZED_VIEWS_PREFIX)[1].split("/") if n != ""]))

            # Get query configs
            blob: Blob = get_blob(blob_name, SENSOR_BUCKET)
            query_config: dict = yaml.safe_load(
                blob.download_as_string().decode("utf-8"))

            # Checks if query is modified and then reset it on Redis
            query_modified = view_config["query_modified"]
            view_config["query_modified"] = False

            # Get base configs
            run_key = build_run_key(blob_name, now)
            with open(str(Path(__file__).parent / "materialized_views_base_config.yaml"), "r") as f:
                base_config: dict = yaml.safe_load(f)
            base_config["run_timestamp"] = "'{}'".format(
                convert_datetime_to_datetime_string(now))
            base_config["maestro_sha"] = "'{}'".format(fetch_branch_sha(
                constants.MAESTRO_REPOSITORY.value, constants.MAESTRO_DEFAULT_BRANCH.value))
            base_config["maestro_bq_sha"] = "'{}'".format(fetch_branch_sha(
                constants.MAESTRO_BQ_REPOSITORY.value, constants.MAESTRO_BQ_DEFAULT_BRANCH.value))
            base_config["run_key"] = "'{}'".format(run_key)

            # Set inputs
            config["solids"]["delete_table_on_query_change"]["inputs"]["table_name"]["value"] = table_name
            config["solids"]["delete_table_on_query_change"]["inputs"]["changed"]["value"] = query_modified
            config["solids"]["create_table_if_not_exists"]["inputs"]["base_query"]["value"] = query
            config["solids"]["create_table_if_not_exists"]["inputs"]["table_name"]["value"] = table_name
            config["solids"]["create_table_if_not_exists"]["inputs"]["base_params"]["value"] = base_config
            config["solids"]["create_table_if_not_exists"]["inputs"]["query_params"]["value"] = query_config
            config["solids"]["create_table_if_not_exists"]["inputs"]["now"]["value"] = convert_datetime_to_datetime_string(
                now)
            config["solids"]["insert_into_table_if_already_existed"]["inputs"]["base_query"]["value"] = query
            config["solids"]["insert_into_table_if_already_existed"]["inputs"]["table_name"]["value"] = table_name
            config["solids"]["insert_into_table_if_already_existed"]["inputs"]["base_params"]["value"] = base_config
            config["solids"]["insert_into_table_if_already_existed"]["inputs"]["query_params"]["value"] = query_config
            config["solids"]["insert_into_table_if_already_existed"]["inputs"]["now"]["value"] = convert_datetime_to_datetime_string(
                now)
            config["solids"]["insert_into_table_if_already_existed"]["inputs"]["last_run"]["value"] = convert_datetime_to_datetime_string(
                view_config["last_run"])

            yield RunRequest(
                run_key=run_key,
                run_config=config
            )

            view_config["last_run"] = now

    # Update Redis with new last run times
    rp.set("managed_materialized_views", managed_materialized_views)
