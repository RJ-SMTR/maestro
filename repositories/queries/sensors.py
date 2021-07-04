import os
from pathlib import Path

from redis_pal import RedisPal
from dagster import RunRequest, sensor, SensorExecutionContext

from repositories.helpers.helpers import read_config
from repositories.helpers.io import build_run_key, get_list_of_files, parse_filepath_to_tablename, parse_run_key

VIEWS_DIRECTORY = os.getenv(
    "VIEWS_DIRECTORY", "/opt/dagster/app/repositories/queries/views")
MAT_VIEWS_DIRECTORY = os.getenv(
    "MAT_VIEWS_DIRECTORY", "/opt/dagster/app/repositories/queries/materialized_views")


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
    list_of_files: list = get_list_of_files(VIEWS_DIRECTORY)

    # Get previous set of files from Redis
    prev_set_of_files: set = rp.get("queries_files_set")

    # Parse current list of files to set
    set_of_files: set = set(list_of_files)

    # If we've cached a previous list of files
    if prev_set_of_files is not None:

        # Get deleted files
        deleted: set = prev_set_of_files - set_of_files
        rp.set("aaa", deleted)

        # For every deleted file, delete corresponding view
        filepath: str = ""
        for filepath in deleted:
            if filepath.endswith(".sql"):

                # Extract table name from file path
                table_name: str = parse_filepath_to_tablename(
                    filepath.split(VIEWS_DIRECTORY)[1].strip('/'))

                # Set a run key so we can keep track of changes
                run_key: str = build_run_key("delete-" + filepath, last_mtime)

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
    rp.set("queries_files_set", set_of_files)

    # Iterate over all SQL files
    filepath: str = ""
    for filepath in list_of_files:
        if os.path.isfile(filepath):

            # Get file's last modification timestamp
            file_mtime = os.stat(filepath).st_mtime

            # If file has modified
            if file_mtime > last_mtime:

                # Extract table name from file path
                table_name: str = parse_filepath_to_tablename(
                    filepath.split(VIEWS_DIRECTORY)[1].strip('/'))

                # Extract query from file
                query: str = ""
                with open(filepath, "r") as f:
                    query = f.read()
                    f.close()

                # Set a run key so we can keep track of changes
                run_key: str = build_run_key(filepath, file_mtime)

                # Load run configuration
                config: dict = read_config(
                    Path(__file__).parent / "views.yaml")

                # Set inputs
                config["solids"]["update_view"]["inputs"]["view_sql"]["value"] = query
                config["solids"]["update_view"]["inputs"]["table_name"]["value"] = table_name
                config["solids"]["update_view"]["inputs"]["delete"]["value"] = False

                # Yield a run request
                yield RunRequest(run_key=run_key, run_config=config)
    pass
