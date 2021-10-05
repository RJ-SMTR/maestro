import os
import time
from typing import List
import yaml
import datetime
from pathlib import Path

import pytz
from redis import Redis
from pottery import Redlock
from redis_pal import RedisPal
from google.cloud.storage.blob import Blob
from dagster.core.definitions.run_request import PipelineRunReaction, SkipReason
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
from repositories.queries.utils import get_largest_blob_mtime, filter_blobs_by_mtime

SENSOR_BUCKET = os.getenv("SENSOR_BUCKET", "rj-smtr-dev")
VIEWS_PREFIX = os.getenv("VIEWS_PREFIX", "queries/views/")
MATERIALIZED_VIEWS_PREFIX = os.getenv(
    "MATERIALIZED_VIEWS_PREFIX", "queries/smtr/")


@sensor(
    pipeline_name="update_managed_materialized_views",
    mode="dev",
    minimum_interval_seconds=constants.MATERIALIZED_VIEWS_UPDATE_SENSOR_MIN_INTERVAL.value,
)
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

    # Get connection to Redis
    rp = RedisPal(host=constants.REDIS_HOST.value)

    # Get list of blobs in bucket
    blobs_list = get_list_of_blobs(MATERIALIZED_VIEWS_PREFIX, SENSOR_BUCKET)

    # Get previous set of blobs in Redis
    previous_blobs_set: set = rp.get(
        constants.REDIS_KEY_MAT_VIEWS_BLOBS_SET.value)

    # If there is no previous set, create it
    if not previous_blobs_set:
        rp.set(constants.REDIS_KEY_MAT_VIEWS_BLOBS_SET.value,
               set([b.name for b in blobs_list]))

    # If there is a previous set, compare it to the current set
    else:
        deleted_blobs: set = previous_blobs_set - \
            set([b.name for b in blobs_list])

    # Get previous run mtime
    previous_run_mtime = rp.get(
        constants.REDIS_KEY_MAT_VIEWS_LAST_RUN_MTIME.value)

    # If there is no previous run mtime, set modified blobs to the current blobs
    if not previous_run_mtime:
        modified_blobs = blobs_list

    # If there is a previous run mtime, compare it to the current list
    # and get modified files
    else:
        modified_blobs = filter_blobs_by_mtime(blobs_list, previous_run_mtime)

    # Update last run time
    largest_mtime = get_largest_blob_mtime(blobs_list)
    rp.set(constants.REDIS_KEY_MAT_VIEWS_LAST_RUN_MTIME.value, largest_mtime)

    # If there are modified or deleted files, trigger pipeline
    if modified_blobs or deleted_blobs:

        # Load run configuration and set inputs
        config: dict = read_config(
            Path(__file__).parent / "materialized_views_update.yaml")
        config["solids"]["delete_managed_views"]["inputs"]["blob_names"]["value"] = list(
            deleted_blobs)
        config["solids"]["update_managed_views"]["inputs"]["blob_names"]["value"] = [
            b.name for b in modified_blobs]

        # Set a run key
        run_key: str = build_run_key(
            "update-managed-views", largest_mtime)

        # Yield a run request
        yield RunRequest(run_key=run_key, run_config=config)

    # If there are no modified or deleted files,
    # skip the pipeline
    else:
        yield SkipReason(
            f"Modified files: {len(modified_blobs)}. Deleted files: {len(deleted_blobs)}")


@sensor(
    pipeline_name="materialize_view",
    mode="dev",
    minimum_interval_seconds=constants.MATERIALIZED_VIEWS_EXECUTE_SENSOR_MIN_INTERVAL.value,
)
def materialized_views_execute_sensor(context: SensorExecutionContext):
    """Sensor for executing materialized views based on cron expressions."""
    # Setup Redis and Redlock
    r = Redis(constants.REDIS_HOST.value)
    lock = Redlock(key=constants.REDIS_KEY_MAT_VIEWS_MATERIALIZE_SENSOR_LOCK.value,
                   auto_release_time=constants.REDIS_LOCK_AUTO_RELEASE_TIME.value, masters=[r])

    if lock.acquire(timeout=2):
        lock.release()
    else:
        yield SkipReason("Another run is already in progress!")
        return

    rp = RedisPal(constants.REDIS_HOST.value)

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
