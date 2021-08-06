import jinja2
from redis_pal import RedisPal
from dagster import solid, RetryPolicy

from repositories.helpers.constants import constants
from repositories.helpers.io import (
    run_query,
    check_if_table_exists,
    insert_results_to_table
)


@solid
def update_materialized_view_on_redis(context, blob_name: str, cron_expression: str, delete: bool, query_modified: bool):
    rp = RedisPal(constants.REDIS_HOST.value)
    materialized_views: dict = rp.get("managed_materialized_views")
    materialized_views = materialized_views if materialized_views else {}
    if query_modified:
        context.log.info(f"Query has been modified!")
    else:
        context.log.info(f"Query has NOT been modified!")
    if (delete and blob_name in materialized_views):
        del materialized_views[blob_name]
        context.log.info(f"Deleted materialized view {blob_name}")
    elif (not delete):
        if (blob_name in materialized_views):
            materialized_views[blob_name]["cron_expression"] = cron_expression
            materialized_views[blob_name]["query_modified"] = query_modified
        else:
            materialized_views[blob_name] = {
                "cron_expression": cron_expression,
                "last_run": None,
                "query_modified": query_modified,
            }
        context.log.info(f"Updated materialized view {blob_name}")
    else:
        context.log.warning(
            f"Materialized view {blob_name} does not exist, skipping...")
    rp.set("managed_materialized_views", materialized_views)


@solid(retry_policy=RetryPolicy(max_retries=3, delay=5))
def delete_table_on_query_change(context, table_name: str, changed: bool):
    if check_if_table_exists(table_name) and changed:
        context.log.info(f"Deleting table {table_name}")
        context.log.info(f"Running query: DROP TABLE {table_name}")
        run_query(f"DROP TABLE {table_name}", timeout=300)
    else:
        context.log.info(
            f"Skipping table {table_name} as it does not exist or query hasn't changed")
    return True


@solid(retry_policy=RetryPolicy(max_retries=3, delay=5))
def create_table_if_not_exists(context, base_query: str, base_params: dict, query_params: dict, table_name: str, now: str, last_step_done: bool):
    """Creates a table if it doesn't exist"""

    # If table does not exist
    if not check_if_table_exists(table_name):

        # Get params
        base_params = base_params["value"]  # Basic parameters
        query_params = query_params["value"]  # Query parameters
        custom_params = {
            "date_range_start": "'{}'".format(query_params["backfill"]["start_timestamp"]),
            "date_range_end": "'{}'".format(now)
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
        return False

    # If table exists
    else:
        context.log.info(
            f"Skipping table {table_name} as it already exists")
        return True


@solid(retry_policy=RetryPolicy(max_retries=3, delay=5))
def insert_into_table_if_already_existed(context, base_query: str, base_params: dict, query_params: dict, table_name: str, last_run: str, now: str, already_existed: bool):
    """Creates a table if it doesn't exist"""

    # If table already existed
    if already_existed:

        # Get params
        base_params = base_params["value"]  # Basic parameters
        query_params = query_params["value"]  # Query parameters
        custom_params = {
            "date_range_start": "'{}'".format(last_run),
            "date_range_end": "'{}'".format(now)
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
            f"Skipping table {table_name} as has been created now")
