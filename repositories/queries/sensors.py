import os
import time
import yaml
import datetime
from pathlib import Path

import pytz
from redis_pal import RedisPal
from google.cloud.storage.blob import Blob
from dagster.core.definitions.run_request import SkipReason
from dagster import RunRequest, sensor, SensorExecutionContext

from repositories.helpers.helpers import read_config
from repositories.helpers.constants import constants
from repositories.helpers.datetime import determine_whether_to_execute_or_not, convert_datetime_to_unix_time
from repositories.helpers.io import (
    build_run_key,
    get_list_of_blobs,
    parse_run_key,
    fetch_branch_sha,
)

SENSOR_BUCKET = os.getenv("SENSOR_BUCKET", "rj-smtr-dev")
VIEWS_PREFIX = os.getenv("VIEWS_PREFIX", "queries/views/")
MATERIALIZED_VIEWS_PREFIX = os.getenv(
    "MATERIALIZED_VIEWS_PREFIX", "queries/smtr/")


@sensor(pipeline_name="update_managed_materialized_views", mode="dev")
def materialized_views_update_sensor(context: SensorExecutionContext):
    """Sensor for updating materialized views on file changes.

    For every new or modified file, the pipeline `update_managed_materialized_views`
    is triggered. This ensures BQ materialized views are always up-to-date.
    """
    # Store largest mtime
    largest_mtime = 0

    # Store deleted and modified blobs
    deleted_blobs = []
    modified_blobs = []

    # Start RedisPal (we use that to check on deleted files)
    rp: RedisPal = RedisPal(host=constants.REDIS_HOST.value)

    # Get last modification time
    last_mtime = parse_run_key(context.last_run_key)[
        1] if context.last_run_key else 0
    largest_mtime = last_mtime

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
        deleted_blobs: list = list(prev_set_of_blobs - set_of_blobs)

    # Cache current file list
    rp.set("materialized_views_files_set", set_of_blobs)

    # Iterate over all files
    blob: Blob = None
    for blob in list_of_blobs:

        # Get file's last modification timestamp
        file_mtime = time.mktime(blob.updated.timetuple())

        # If file has modified
        if file_mtime > last_mtime:
            modified_blobs.append(blob.name)
            largest_mtime = max(largest_mtime, file_mtime)

    # If there are modified files, trigger update_managed_views
    if len(modified_blobs) > 0 or len(deleted_blobs) > 0:

        # Load run configuration and set inputs
        config: dict = read_config(
            Path(__file__).parent / "materialized_views_update.yaml")
        config["solids"]["delete_managed_views"]["inputs"]["blob_names"]["value"] = deleted_blobs
        config["solids"]["update_managed_views"]["inputs"]["blob_names"]["value"] = modified_blobs

        # Set a run key
        run_key: str = build_run_key(
            "update-managed-views", largest_mtime)

        # Yield a run request
        yield RunRequest(run_key=run_key, run_config=config)

    else:
        # If no files have changed, skip
        yield SkipReason(
            "No files have changed since last execution.")


@sensor(pipeline_name="materialize_view", mode="dev")
def materialized_views_execute_sensor(context: SensorExecutionContext):
    """Sensor for executing materialized views based on cron expressions."""
    # Start RedisPal (we use that to check cron expressions)
    rp: RedisPal = RedisPal(host=constants.REDIS_HOST.value)

    # Get managed materialized views
    managed_materialized_views: dict = rp.get("managed_materialized_views")
    if managed_materialized_views is None:
        managed_materialized_views = {}
        managed_materialized_views["views"] = {}

    # Get current timestamp
    now = datetime.datetime.now(pytz.timezone("America/Sao_Paulo"))

    # Iterate over all managed materialized views, storing a list
    # of all queries to be executed
    queries_to_execute: list = []
    for blob_name, view_config in managed_materialized_views["views"].items():
        if (view_config["last_run"] is None or
                    determine_whether_to_execute_or_not(
                    view_config["cron_expression"], now, view_config["last_run"])
                ) and (view_config["materialized"]):
            # Add to list of queries to execute
            queries_to_execute.append(blob_name)

    # Launch run if we have any queries to execute
    if queries_to_execute:
        # Get run configuration
        config: dict = read_config(
            Path(__file__).parent / "materialized_views_execute.yaml")

        # Get run key
        run_key = build_run_key("materialized_views_execute", now)

        # Set inputs
        config["solids"]["resolve_dependencies_and_execute"]["inputs"]["queries_names"]["value"] = queries_to_execute

        yield RunRequest(
            run_key=run_key,
            run_config=config
        )

    # Tell Dagit a reason we skipped it
    else:
        yield SkipReason("No materialization requested for now")
