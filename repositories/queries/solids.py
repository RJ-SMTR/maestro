import os
import datetime
from pathlib import Path
from typing import Any, List

import pytz
import yaml
import jinja2
import networkx as nx
from redis import Redis
from pottery import Redlock
from redis_pal import RedisPal
from dagster import solid, RetryPolicy
from dagster.experimental import DynamicOutputDefinition, DynamicOutput

from repositories.helpers.constants import constants
from repositories.helpers.datetime import convert_datetime_to_datetime_string, get_date_ranges
from repositories.helpers.helpers import remove_duplicates
from repositories.queries.sensors import MATERIALIZED_VIEWS_PREFIX, SENSOR_BUCKET
from repositories.helpers.io import (
    build_run_key,
    fetch_branch_sha,
    get_blob,
    get_table_type,
    parse_filepath_to_tablename,
    run_query,
    test_query,
    check_if_table_exists,
    insert_results_to_table,
    update_view,
)
from repositories.helpers.hooks import log_critical
from repositories.helpers import locks
from repositories.queries.utils import (
    check_mat_view_dict_format,
    update_dict_with_dict,
    replace_table_name_with_query,
)


@solid
def get_materialization_lock(context):
    """
    Get a lock for the materialization process.
    """
    r = Redis(constants.REDIS_HOST.value)
    lock = Redlock(
        key=constants.REDIS_KEY_MAT_VIEWS_MATERIALIZE_LOCK.value, masters=[
            r],
        auto_release_time=constants.REDIS_LOCK_AUTO_RELEASE_TIME.value,
    )
    return lock


@solid
def get_materialize_sensor_lock(context):
    """
    Get a lock for the materialization sensor.
    """
    r = Redis(constants.REDIS_HOST.value)
    lock = Redlock(
        key=constants.REDIS_KEY_MAT_VIEWS_MATERIALIZE_SENSOR_LOCK.value, masters=[
            r],
        auto_release_time=constants.REDIS_LOCK_AUTO_RELEASE_TIME.value,
    )
    return lock


@solid
def gather_locks(context, lock_a, lock_b):
    """
    Gather all locks for the materialization process.
    """
    return [lock_a, lock_b]


@solid
def lock_materialization_process(context, lock):
    """
    Lock the materialization process.
    """
    context.log.info("Locking materialization process.")
    locks.acquire(lock)
    context.log.info("Locked materialization process.")
    return True


@solid
def release_materialization_process(context, lock, done: Any):
    """
    Release the materialization process.
    """
    context.log.info("Releasing materialization process.")
    locks.release(lock)
    context.log.info("Released materialization process.")
    return True


@solid(retry_policy=RetryPolicy(max_retries=3, delay=5))
def delete_managed_views(
    context,
    blob_names,
    materialization_locked: bool,
    materialization_lock: Redlock,
):
    try:
        r = Redis(constants.REDIS_HOST.value)
        rp = RedisPal(constants.REDIS_HOST.value)
        lock = Redlock(
            key=constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS_LOCK.value, masters=[
                r],
            auto_release_time=constants.REDIS_LOCK_AUTO_RELEASE_TIME.value,
        )
        with lock:
            materialized_views: dict = rp.get(
                constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value)
            if materialized_views is None:
                materialized_views = {}
                materialized_views["views"] = {}
            for blob_name in blob_names:
                context.log.info(f"Deleting managed view {blob_name}")
                if blob_name in materialized_views["views"]:
                    del materialized_views["views"][blob_name]
                    prefix: str = os.getenv("BQ_PROJECT_NAME", "rj-smtr-dev")
                    table_name: str = f"{prefix}.{blob_name}"
                    update_view(table_name, {}, "", "", "", delete=True)
                    context.log.info("Success!")
                else:
                    context.log.info("View not found, skipping...")
            rp.set(constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value,
                   materialized_views)
    except:
        materialization_lock.release()
        raise


@solid(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
    output_defs=[DynamicOutputDefinition(dict)],
)
def update_managed_views(
    context,
    blob_names,
    materialization_locked: bool,
    materialization_lock: Redlock,
):
    try:
        # Setup Redis and Redlock
        r = Redis(constants.REDIS_HOST.value)
        rp = RedisPal(constants.REDIS_HOST.value)
        views_lock = Redlock(
            key=constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS_LOCK.value, masters=[
                r],
            auto_release_time=constants.REDIS_LOCK_AUTO_RELEASE_TIME.value,
        )

        # Initialize graph
        graph = nx.DiGraph()

        # If blob_name ends with "defaults.yaml", we need to
        # either add it to Redis or update its values and add
        # runs for every child it has and its dependencies.
        for blob_name in [b for b in blob_names if b.endswith("defaults.yaml")]:

            # Get dataset name
            blob_path = "/".join([n for n in blob_name.split("/")
                                  if n != ""][:-1])
            dataset_name: str = blob_path.split("/")[-1]

            context.log.info("#" * 80)
            context.log.info(f"Updating {dataset_name} defaults")

            # Read the blob
            blob = get_blob(blob_name, SENSOR_BUCKET, mode="staging")
            if blob is None:
                raise Exception(f"Blob {blob_name} not found")
            blob_dict: dict = yaml.safe_load(blob.download_as_string())

            # Add it to Redis
            with views_lock:
                materialized_views: dict = rp.get(
                    constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value)
                if materialized_views is None:
                    materialized_views = {}
                    materialized_views["views"] = {}
                # Add every child to Redis
                if "views" not in blob_dict:
                    raise Exception(
                        f"Malformed blob (missing views key): {blob_name}")
                for key in blob_dict["views"].keys():

                    # Build key with dataset_name
                    m_key = f"{dataset_name}.{key}"

                    # This child also needs a run
                    context.log.info(f"Adding {m_key} to runs")
                    if m_key not in graph.nodes:
                        graph.add_node(m_key)

                    # Avoid KeyError
                    if "views" not in materialized_views:
                        materialized_views["views"] = {}

                    # Add to Redis
                    if m_key not in materialized_views["views"]:
                        materialized_views["views"][m_key] = {}
                    update_dict_with_dict(materialized_views["views"][m_key], {
                        "cron_expression": blob_dict["scheduling"]["cron"],
                        "last_run": None,
                        "materialized": blob_dict["views"][key]["materialized"],
                        "query_modified": True,
                        "depends_on": blob_dict["views"][key]["depends_on"],
                    })

                    # Adds dependencies to runs
                    for dep in blob_dict["views"][key]["depends_on"]:
                        context.log.info(
                            f"Adding {dep} to runs as dependency of {m_key}")
                        if dep not in graph.nodes:
                            graph.add_node(dep)
                        graph.add_edge(dep, m_key)

                    # Try to find specific values for this view
                    blob = get_blob(blob_path + key + ".yaml",
                                    SENSOR_BUCKET, mode="staging")
                    if blob:
                        # Replace values in Redis
                        specific = yaml.safe_load(
                            blob.download_as_string().decode("utf-8"))
                        materialized_views["views"][m_key]["cron_expression"] = specific[
                            "scheduling"]["cron"]
                    else:
                        context.log.warning(
                            f"No specific values for {m_key} found. This is not an error.")

                # Update Redis effectively
                rp.set(constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value,
                       materialized_views)

        # Otherwise, we need to add the blob_name and its
        # dependencies to the graph.
        for blob_name in [b for b in blob_names if not b.endswith("defaults.yaml")]:

            # Get table name
            file_name = ".".join(blob_name.split("/")[-2:])
            table_name = ".".join(file_name.split(".")[:-1])

            context.log.info("#" * 80)
            context.log.info(f"Updating {table_name} specific values...")

            # If it's YAML file, update values on Redis
            if blob_name.endswith(".yaml"):

                # Read the blob
                blob = get_blob(blob_name, SENSOR_BUCKET, mode="staging")
                if blob is None:
                    raise Exception(f"Blob {blob_name} not found")
                blob_dict: dict = yaml.safe_load(blob.download_as_string())

                # Update Redis
                with views_lock:
                    materialized_views: dict = rp.get(
                        constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value)
                    if materialized_views is None:
                        materialized_views = {}
                        materialized_views["views"] = {}

                    if table_name not in materialized_views["views"]:
                        materialized_views["views"][table_name] = {}
                    update_dict_with_dict(materialized_views["views"][table_name], {
                        "cron_expression": blob_dict["scheduling"]["cron"],
                        "last_run": None,
                        "query_modified": True,
                    })
                    rp.set(
                        constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value, materialized_views)

            # Add table_name and its dependencies to runs
            context.log.info(f"Adding {table_name} to runs")
            if table_name not in graph.nodes:
                graph.add_node(table_name)

            materialized_views: dict = rp.get(
                constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value)
            if materialized_views is None:
                materialized_views = {}
                materialized_views["views"] = {}
            if table_name in materialized_views["views"]:
                for dep in materialized_views["views"][table_name]["depends_on"]:
                    context.log.info(
                        f"Adding {dep} to runs as dependency of {table_name}")
                    if dep not in graph.nodes:
                        graph.add_node(dep)
                    graph.add_edge(dep, table_name)

        context.log.info(f"Graph edges: {graph.edges()}")

        # Get topological order
        order = list(nx.topological_sort(graph))

        # Filter out views that are not on materialized_views["views"]
        order = [o for o in order if o in materialized_views["views"]]

        # Log topological order
        context.log.info(f"Order: {order}")

        # Execute queries in topological order
        for q in order:
            yield DynamicOutput({"view_name": q, "materialization_lock": materialization_lock}, mapping_key=q.replace(".", "_"))
    except:
        materialization_lock.release()
        raise


@solid(retry_policy=RetryPolicy(max_retries=3, delay=30))
def manage_view(context, input_dict):

    view_name = input_dict["view_name"]
    materialization_lock = input_dict["materialization_lock"]

    try:
        # Setup Redis and Redlock
        r = Redis(constants.REDIS_HOST.value)
        rp = RedisPal(constants.REDIS_HOST.value)
        lock = Redlock(
            key=constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS_LOCK.value, masters=[
                r],
            auto_release_time=constants.REDIS_LOCK_AUTO_RELEASE_TIME.value,
        )

        # Get materialization information from Redis
        materialized_views: dict = rp.get(
            constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value)
        if materialized_views is None:
            materialized_views = {}
            materialized_views["views"] = {}
        materialized = materialized_views["views"][view_name]["materialized"]

        # If this is materialized, generate temp view
        if materialized:
            with lock:
                materialized_views["views"][view_name]["query_modified"] = True
                materialized_views["views"][view_name]["last_run"] = None
                rp.set(constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value,
                       materialized_views)
            context.log.info(
                f"Generate {view_name} as a view for now, materialization comes later")

        # We need to build the query using
        # latest parameters and build a view with it.

        # Get defaults for view_name
        blob_path = os.path.join(*([MATERIALIZED_VIEWS_PREFIX] +
                                   [n for n in view_name.split(".")][:-1]))
        defaults_path = blob_path + "/defaults.yaml"
        context.log.info(f"Defaults path -> {defaults_path}")
        defaults_blob = get_blob(defaults_path, SENSOR_BUCKET, mode="staging")
        if defaults_blob is None:
            raise Exception(f"Blob {defaults_path} not found")
        defaults_dict: dict = yaml.safe_load(
            defaults_blob.download_as_string())

        # Parse dataset_name
        dataset_name = view_name.split(".")[0]

        # Parse view yaml path
        view_yaml = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, view_name)}.yaml'

        # Parse table_name
        prefix: str = os.getenv("BQ_PROJECT_NAME", "rj-smtr-dev")
        table_name: str = f"{prefix}.{view_name}"
        context.log.info(f"Table name is {table_name}")

        # Update view
        update_view(table_name, defaults_dict, dataset_name, view_name.split(".")[-1],
                    view_yaml, delete=False, context=context)

    except:
        materialization_lock.release()
        raise


@solid(retry_policy=RetryPolicy(max_retries=3, delay=30))
def materialize(context, input_dict: dict):

    config_dict = input_dict["config_dict"]
    materialization_lock = input_dict["materialization_lock"]
    try:
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
        parent_queries = config_dict["parent_queries"]

        ###########
        # Step 1: Delete table on query change
        ###########

        if check_if_table_exists(table_name) and changed:
            context.log.info(f"Deleting table {table_name}")
            delete_query = f"DROP {get_table_type(table_name)} `{table_name}`"
            context.log.info(f"Running query: {delete_query}")
            err = test_query(delete_query)
            if err:
                log_critical(f"Failed to delete table {table_name}: {err}")
                raise Exception(f"Failed to delete table {table_name}: {err}")
            run_query(delete_query, timeout=300)

        else:
            context.log.info(
                f"Skipping DELETE table {table_name} as it does not exist or query hasn't changed.\nTable exists={check_if_table_exists(table_name)}, changed={changed}")

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
            context.log.info(f"Query before deps -> {query}")

            # Build query dependencies
            for parent_query in parent_queries:
                context.log.info(
                    f'For parent table {parent_query}, args are {dict(**base_params, **parent_queries[parent_query]["query_params"]["parameters"], **custom_params)}')
                context.log.info(
                    f'For parent table {parent_query}, query is {parent_queries[parent_query]["base_query"]}')
                parent_queries[parent_query]["rendered_query"] = jinja2.Template(
                    parent_queries[parent_query]["base_query"]).render(
                        **base_params, **parent_queries[parent_query]["query_params"]["parameters"], **custom_params)
                context.log.info(
                    f'Parent query {parent_query} -> {parent_queries[parent_query]["rendered_query"]}')

            # Replace parent queries
            for parent_query in parent_queries:
                query, count = replace_table_name_with_query(
                    parent_query, parent_queries[parent_query]["rendered_query"], query)
                context.log.info(
                    f"Replaced {count} occurences of parent table {parent_query}")
            context.log.info(f"Query -> {query}")

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
            err = test_query(create_table_query)
            if err:
                log_critical(f"Failed to create table {table_name}: {err}")
                raise Exception(f"Failed to create table {table_name}: {err}")
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

            # Build query dependencies
            for parent_query in parent_queries:
                context.log.info(
                    f'For parent table {parent_query}, args are {dict(**base_params, **parent_queries[parent_query]["query_params"]["parameters"], **custom_params)}')
                parent_queries[parent_query]["rendered_query"] = jinja2.Template(
                    parent_queries[parent_query]["base_query"]).render(
                        **base_params, **parent_queries[parent_query]["query_params"]["parameters"], **custom_params)
                context.log.info(
                    f'Parent query {parent_query} -> {parent_queries[parent_query]["rendered_query"]}')

            # Replace parent queries
            for parent_query in parent_queries:
                query, count = replace_table_name_with_query(
                    parent_query, parent_queries[parent_query]["rendered_query"], query)
                context.log.info(
                    f"Replaced {count} occurences of parent table {parent_query}")
            context.log.info(f"Query -> {query}")

            # Insert into query
            insert_query_template = jinja2.Template(
                """
                INSERT INTO {{ table_name }}
                ({{ query }})
                """
            )
            insert_query = insert_query_template.render(
                table_name=table_name, query=query)

            # Execute query
            context.log.info(f"Running query: {insert_query}")
            err = test_query(insert_query)
            if err:
                log_critical(
                    f"Failed to insert into table {table_name}: {err}")
                raise Exception(
                    f"Failed to insert into table {table_name}: {err}")
            results = run_query(insert_query, timeout=1800)
            context.log.info(f"Results: {results.to_dataframe()}")

        else:
            context.log.info(
                f"Skipping INSERT INTO table {table_name} as has been created now")
    except:
        locks.release(materialization_lock)
        raise


@solid(
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    output_defs=[DynamicOutputDefinition(dict)]
)
def get_configs_for_materialized_view(context, query_names: list, materialization_locked: bool, materialization_lock) -> dict:
    """Retrieves configs for materialized views"""
    try:
        for query_name in query_names:

            # Split query name into dataset_name and view_name
            dataset_name, view_name = query_name.split(".")

            # Load configs from GCS
            view_yaml = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, dataset_name, view_name)}.yaml'
            defaults_yaml = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, dataset_name)}/defaults.yaml'
            context.log.info(f"Defaults blob: {defaults_yaml}")
            context.log.info(f"View blob: {view_yaml}")
            defaults_blob = get_blob(
                defaults_yaml, SENSOR_BUCKET, mode="staging")
            view_blob = get_blob(view_yaml, SENSOR_BUCKET, mode="staging")
            if defaults_blob is None:
                raise Exception(f"Blob {defaults_yaml} not found!")
            defaults_dict = yaml.safe_load(defaults_blob.download_as_string())
            if view_blob:
                view_dict = yaml.safe_load(view_blob.download_as_string())
            else:
                context.log.warning(
                    f"Blob {view_yaml} not found. This is not an error.")
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
            lock = Redlock(
                key=constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS_LOCK.value, masters=[
                    r],
                auto_release_time=constants.REDIS_LOCK_AUTO_RELEASE_TIME.value,
            )
            table_name = parse_filepath_to_tablename(view_yaml)
            with lock:
                managed = rp.get(
                    constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value)
                if managed is None:
                    managed = {}
                    managed["views"] = {}
                if query_name not in managed["views"]:
                    raise Exception(
                        f"Query {query_name} not found in managed views: {managed}")
                d = managed["views"][query_name]
                changed = d["query_modified"]
                context.log.info(f"{query_name} changed: {changed}")
                d["query_modified"] = False
                last_run = d["last_run"]
                d["last_run"] = now
                rp.set(constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value, managed)

            # Get query on GCS
            query_file = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, dataset_name, view_name)}.sql'
            query_blob = get_blob(query_file, SENSOR_BUCKET, mode="staging")
            if query_blob is None:
                raise Exception(f"Blob {query_file} not found!")
            base_query = query_blob.download_as_string().decode("utf-8")

            # Get parent queries on GCS
            parent_queries = {}
            for query_name in d["depends_on"]:
                query_file = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, "/".join(query_name.split(".")[:2]))}.sql'
                query_blob = get_blob(
                    query_file, SENSOR_BUCKET, mode="staging")
                if query_blob is None:
                    context.log.warning(
                        f"Blob for parent query \"{query_file}\" not found, skipping...")
                    continue
                parent_view_yaml = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, "/".join(query_name.split(".")[:2]))}.yaml'
                parent_view_blob = get_blob(
                    parent_view_yaml, SENSOR_BUCKET, mode="staging")
                if parent_view_blob is not None:
                    parent_view_dict = yaml.safe_load(
                        parent_view_blob.download_as_string())
                else:
                    parent_view_dict = {}
                parent_defaults_yaml = f'{os.path.join(MATERIALIZED_VIEWS_PREFIX, "/".join(query_name.split(".")[:1]))}/defaults.yaml'
                parent_defaults_blob = get_blob(
                    parent_defaults_yaml, SENSOR_BUCKET, mode="staging")
                if parent_defaults_blob is not None:
                    parent_defaults_dict = yaml.safe_load(
                        parent_defaults_blob.download_as_string())
                else:
                    context.log.warning(
                        f"Blob for parent query \"{parent_defaults_yaml}\" not found, skipping...")
                    continue
                parent_queries[query_name] = {}
                parent_queries[query_name]["base_query"] = query_blob.download_as_string().decode(
                    "utf-8")
                parent_queries[query_name]["query_params"] = {
                    **parent_defaults_dict, **parent_view_dict}
            context.log.info(f"Parent queries: {parent_queries}")

            # Build configs
            # - table_name: str
            # - changed: bool
            # - base_query: str
            # - base_params: dict
            # - query_params: dict
            # - now: str
            # - last_run: str
            date_ranges = get_date_ranges(
                last_run if last_run else query_params["backfill"]["start_timestamp"],
                query_params["backfill"]["interval"],
                now
            )
            context.log.info(f"{date_ranges}")
            for i, _ in enumerate(date_ranges[:-1]):
                configs = {
                    "table_name": table_name,
                    "changed": changed if i == 0 else False,
                    "base_query": base_query,
                    "base_params": base_params,
                    "query_params": query_params,
                    "now": date_ranges[i + 1],
                    "last_run": date_ranges[i],
                    "parent_queries": parent_queries,
                }
                yield DynamicOutput(
                    {"config_dict": configs,
                        "materialization_lock": materialization_lock},
                    mapping_key=f'{configs["table_name"]}_{configs["last_run"]}_{configs["now"]}'.replace(".", "_").replace("-", "_").replace(" ", "_").replace(":", "_"))
    except:
        locks.release(materialization_lock)
        raise


@solid(
    retry_policy=RetryPolicy(max_retries=3, delay=5),
    output_defs=[DynamicOutputDefinition(str)]
)
def resolve_dependencies_and_execute(context, queries_names, materialization_locked: bool, materialization_lock):

    try:
        # Setup directed graph for DAG sorting
        graph = nx.DiGraph()

        # Get dependencies
        dependencies = {}
        rp = RedisPal(constants.REDIS_HOST.value)
        materialized_views: dict = rp.get(
            constants.REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS.value)
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
    except:
        locks.release(materialization_lock)
        raise
