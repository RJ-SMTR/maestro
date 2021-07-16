import os
import time
from pathlib import Path
from google.cloud.storage.blob import Blob

from redis_pal import RedisPal
from dagster import RunRequest, sensor, SensorExecutionContext

from repositories.helpers.helpers import read_config
from repositories.helpers.io import build_run_key, get_list_of_blobs, parse_filepath_to_tablename, parse_run_key

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
    rp: RedisPal = RedisPal(host="dagster-redis-master")

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
