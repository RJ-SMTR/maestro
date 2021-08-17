import pytz
import yaml
import jinja2
import datetime
import networkx as nx
from redis import Redis
from pathlib import Path
from pottery import Redlock
from redis_pal import RedisPal
from dagster import solid, RetryPolicy
from dagster.experimental import DynamicOutputDefinition, DynamicOutput

from repositories.helpers.constants import constants
from repositories.helpers.datetime import convert_datetime_to_datetime_string
from repositories.queries.sensors import SENSOR_BUCKET
from repositories.helpers.io import (
    build_run_key,
    fetch_branch_sha,
    get_blob,
    parse_filepath_to_tablename,
    run_query,
    check_if_table_exists,
    insert_results_to_table
)


@solid
def update_materialized_view_on_redis(
    context,
    blob_name: str,
    cron_expression: str,
    delete: bool,
    query_modified: bool,
    defaults_yaml: bool,
    defaults_dict: dict,
    dataset_name: str,
):
    r = Redis(constants.REDIS_HOST.value)
    rp = RedisPal(constants.REDIS_HOST.value)
    lock = Redlock(key="lock_managed_materialized_views", masters=[r])
    defaults_dict = defaults_dict["value"]
    blob_path = "/".join(blob_name.split("/")[:-1])
    blob_name_without_dataset = ".".join(blob_name.split(".")[:-1])
    blob_name = f'{dataset_name}.{blob_name_without_dataset.split("/")[-1]}'

    with lock:
        materialized_views: dict = rp.get("managed_materialized_views")
        materialized_views = materialized_views if materialized_views else {
            "views": {}}
        # Modified YAML files
        # If defaults.yaml
        if defaults_yaml:
            for key in defaults_dict["views"].keys():
                m_key = f"{dataset_name}.{key}"
                materialized_views["views"][m_key] = {
                    "cron_expression": defaults_dict["scheduling"]["cron"],
                    "last_run": None,
                    "materialized": defaults_dict["views"][key]["materialized"],
                    "query_modified": False,
                    "depends_on": defaults_dict["views"][key]["depends_on"],
                }
                blob = get_blob(blob_path + key + ".yaml",
                                SENSOR_BUCKET, mode="staging")
                if blob:
                    specific = yaml.safe_load(
                        blob.download_as_string().decode("utf-8"))
                    materialized_views["views"][m_key]["cron_expression"] = specific[
                        "scheduling"]["cron"]
        # Any other YAML file will provide a cron_expression
        # Also valid for modified SQL files
        elif cron_expression != "" or query_modified:
            # Check if it exists on dict
            if blob_name in materialized_views["views"]:
                # If exists, update it
                if cron_expression:
                    materialized_views["views"][blob_name]["cron_expression"] = cron_expression
                else:
                    materialized_views["views"][blob_name]["query_modified"] = query_modified
            else:
                # If not, create it
                cron_expression = cron_expression if cron_expression != "" else defaults_dict[
                    "scheduling"]["cron"]
                materialized_views["views"][blob_name] = {
                    "cron_expression": cron_expression,
                    "last_run": None,
                    "materialized": defaults_dict[blob_name]["materialized"],
                    "query_modified": query_modified,
                    "depends_on": defaults_dict[blob_name]["depends_on"],
                }
        # If deleted SQL file
        elif delete and blob_name in materialized_views["views"]:
            del materialized_views["views"][blob_name]
        rp.set("managed_materialized_views", materialized_views)


@solid(retry_policy=RetryPolicy(max_retries=3, delay=5))
def materialize(context, config_dict: dict):

    ###########
    # Step 0: Extract config
    ###########
    # - table_name: str
    # - changed: bool
    # - base_query: str
    # - base_params: dict
    # - query_params: dict
    # - now: str
    # - last_run: str
    table_name = config_dict["table_name"]
    changed = config_dict["changed"]
    base_query = config_dict["base_query"]
    base_params = config_dict["base_params"]
    query_params = config_dict["query_params"]
    now = config_dict["now"]
    last_run = config_dict["last_run"]

    ###########
    # Step 1: Delete table on query change
    ###########

    if check_if_table_exists(table_name) and changed:
        context.log.info(f"Deleting table {table_name}")
        context.log.info(f"Running query: DROP TABLE {table_name}")
        run_query(f"DROP TABLE {table_name}", timeout=300)
    else:
        context.log.info(
            f"Skipping DELETE table {table_name} as it does not exist or query hasn't changed")

    ###########
    # Step 2: Create table if not exists
    ###########

    # If table does not exist
    if not check_if_table_exists(table_name):

        # Get params
        custom_params = {
            "date_range_start": "'{}'".format(query_params["backfill"]["start_timestamp"]),
            "date_range_end": "'{}'".format(convert_datetime_to_datetime_string(now))
        }  # Backfill parameters

        # Build query for data
        template = jinja2.Template(base_query)
        query = template.render(
            **base_params, **query_params["parameters"], **custom_params)

        # Build CREATE TABLE query
        partition_by_type: str = query_params["partitioning"]["type"]
        if partition_by_type.upper() != "DATE":
            partition_by_period = query_params["partitioning"]["period"]
        else:
            partition_by_period = None
        create_table_query = f"""
        CREATE TABLE {table_name}
            PARTITION BY {partition_by_type}({query_params["partitioning"]["column"]}{f", {partition_by_period}" if partition_by_period else ""})
            AS
            ({query})
        """

        # Run query
        context.log.info(f"Running query: {create_table_query}")
        run_query(create_table_query, timeout=1800)
        already_existed = False

    # If table exists
    else:
        context.log.info(
            f"Skipping CREATE table {table_name} as it already exists")
        already_existed = True

    ###########
    # Step 3: Insert into table if already existed
    ###########

    # If table already existed
    if already_existed:

        # Get params
        custom_params = {
            "date_range_start": "'{}'".format(convert_datetime_to_datetime_string(last_run)),
            "date_range_end": "'{}'".format(convert_datetime_to_datetime_string(now))
        }  # Backfill parameters

        # Build query for data
        template = jinja2.Template(base_query)
        query = template.render(
            **base_params, **query_params["parameters"], **custom_params)

        # Execute query
        context.log.info(f"Running query: {query}")
        results = run_query(query, timeout=1800)

        # Insert results to table and check for errors
        if (results.total_rows == 0):
            context.log.warning(
                f"No rows found for query, skipping...")
        else:
            errors = insert_results_to_table(results, table_name)
            if errors:
                context.log.error(f"Errors: {errors}")
            else:
                context.log.info(
                    f"Inserted {results.total_rows} rows into {table_name}")

    else:
        context.log.info(
            f"Skipping INSERT INTO table {table_name} as has been created now")


@solid(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def get_configs_for_materialized_view(context, query_name: str) -> dict:
    """Retrieves configs for a materialized view"""

    # Split query name into dataset_name and view_name
    dataset_name, view_name = query_name.split(".")

    # Load configs from GCS
    view_yaml = f'queries/materialized_views/{dataset_name}/{view_name}.yaml'
    defaults_yaml = f'queries/materialized_views/{dataset_name}/defaults.yaml'
    defaults_blob = get_blob(defaults_yaml, SENSOR_BUCKET, mode="staging")
    view_blob = get_blob(view_yaml, SENSOR_BUCKET, mode="staging")
    defaults_dict = yaml.safe_load(defaults_blob.download_as_string())
    if view_blob:
        view_dict = yaml.safe_load(view_blob.download_as_string())
    else:
        view_dict = {}

    # Merge configs
    query_params = {**defaults_dict, **view_dict}

    # Build base configs
    now = datetime.datetime.now(pytz.timezone("America/Sao_Paulo"))
    run_key = build_run_key(query_name, now)
    with open(str(Path(__file__).parent / "materialized_views_base_config.yaml"), "r") as f:
        base_params: dict = yaml.safe_load(f)
    base_params["run_timestamp"] = "'{}'".format(
        convert_datetime_to_datetime_string(now))
    base_params["maestro_sha"] = "'{}'".format(fetch_branch_sha(
        constants.MAESTRO_REPOSITORY.value, constants.MAESTRO_DEFAULT_BRANCH.value))
    base_params["maestro_bq_sha"] = "'{}'".format(fetch_branch_sha(
        constants.MAESTRO_BQ_REPOSITORY.value, constants.MAESTRO_BQ_DEFAULT_BRANCH.value))
    base_params["run_key"] = "'{}'".format(run_key)

    # Few more params
    r = Redis(constants.REDIS_HOST.value)
    rp = RedisPal(constants.REDIS_HOST.value)
    lock = Redlock(key="lock_managed_materialized_views", masters=[r])
    table_name = parse_filepath_to_tablename(view_yaml)
    with lock:
        managed = rp.get("managed_materialized_views")
        d = managed["views"][query_name]
        changed = d["query_modified"]
        d["query_modified"] = False
        last_run = d["last_run"]
        d["last_run"] = now
        rp.set("managed_materialized_views", managed)

    # Get query on GCS
    query_file = f'queries/materialized_views/{dataset_name}/{view_name}.sql'
    query_blob = get_blob(query_file, SENSOR_BUCKET, mode="staging")
    base_query = query_blob.download_as_string().decode("utf-8")

    # Build configs
    # - table_name: str
    # - changed: bool
    # - base_query: str
    # - base_params: dict
    # - query_params: dict
    # - now: str
    # - last_run: str
    configs = {
        "table_name": table_name,
        "changed": changed,
        "base_query": base_query,
        "base_params": base_params,
        "query_params": query_params,
        "now": now,
        "last_run": last_run
    }

    return configs


@solid(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
    output_defs=[DynamicOutputDefinition(str)]
)
def resolve_dependencies_and_execute(context, queries_names):

    # Setup directed graph for DAG sorting
    graph = nx.DiGraph()

    # Get dependencies
    dependencies = {}
    rp = RedisPal(constants.REDIS_HOST.value)
    materialized_views: dict = rp.get("managed_materialized_views")
    if materialized_views:
        for query_name in queries_names:
            if query_name in materialized_views["views"] and materialized_views["views"][query_name]["materialized"]:
                graph.add_node(query_name)
                dependencies[query_name] = materialized_views["views"][query_name]["depends_on"]
            else:
                context.log.warning(
                    f"{query_name} not found on Redis! Skipping...")

    # Log dependencies
    context.log.info(f"Dependencies: {dependencies}")

    # Add edges to graph
    for query_name in queries_names:
        if query_name in dependencies:
            for dep in dependencies[query_name]:
                if dep in graph.nodes:
                    graph.add_edge(dep, query_name)

    context.log.info(f"Graph: {graph.edges()}")

    # Get topological order
    order = list(nx.topological_sort(graph))

    # Log topological order
    context.log.info(f"Order: {order}")

    # Execute queries in topological order
    for q in order:
        yield DynamicOutput(q, mapping_key=q.replace(".", "_"))
