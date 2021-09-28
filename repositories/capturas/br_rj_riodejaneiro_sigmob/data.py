import shutil
import traceback
from pathlib import Path

import jinja2
import requests
import pandas as pd
from basedosdados import Table
from dagster import solid, pipeline, ModeDefinition, RetryPolicy

from repositories.helpers.hooks import log_critical
from repositories.helpers.constants import constants
from repositories.capturas.resources import endpoints
from repositories.analises.resources import schedule_run_date
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client


def generate_df_and_save(data: dict, fname: Path):
    # Generate dataframe
    df = pd.DataFrame()
    df[data["key_column"]] = [
        piece[data["key_column"]] for piece in data["data"]
    ]
    df["content"] = [piece for piece in data["data"]]

    # Save dataframe to CSV
    df.to_csv(fname, index=False)


@solid(
    required_resource_keys={"endpoints", "schedule_run_date"},
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def request_data(context):

    # Get resources
    run_date = context.resources.schedule_run_date["date"]
    endpoints = context.resources.endpoints["endpoints"]

    # Initialize empty dict for storing file paths
    paths_dict = {}

    # Iterate over endpoints
    for key in endpoints.keys():
        context.log.info("#" * 80)
        context.log.info(f"KEY = {key}")

        # Start with empty contents, page count = 0 and id = 0
        contents = None
        id = 0
        page_count = 0

        # Setup a template for every CSV file
        path_template = jinja2.Template(
            "{{ run_date }}/{{ key }}/data_versao={{ run_date }}/{{ key }}_version-{{ run_date }}-{{ id }}.csv")

        # The first next is the initial URL
        next = endpoints[key]["url"]

        # Iterate over pages
        while next:
            page_count += 1

            try:

                # Get data
                context.log.info(f"URL = {next}")
                data = requests.get(
                    next, timeout=constants.SIGMOB_GET_REQUESTS_TIMEOUT.value)

                # Raise exception if not 200
                data.raise_for_status()
                data = data.json()

                # Store contents
                if contents is None:
                    contents = {
                        "data": data["result"] if "result" in data else data["data"],
                        "key_column": endpoints[key]["key_column"],
                    }
                else:
                    contents["data"].extend(data["data"])

                # Get next page
                if "next" in data and data["next"] != "EOF" and data["next"] != "":
                    next = data["next"]
                else:
                    next = None

            except Exception as e:
                err = traceback.format_exc()
                log_critical(f"Failed to request data from SIGMOB: \n{err}")
                raise e

            # Create a new file for every (constants.SIGMOB_PAGES_FOR_CSV_FILE.value) pages
            if page_count % constants.SIGMOB_PAGES_FOR_CSV_FILE.value == 0:

                # Increment file ID
                id += 1
                #"{{ run_date }}/{{ key }}/data_versao={{ run_date }}/{{ key }}_version-{{ run_date }}-{{ id }}.csv"
                path = Path(path_template.render(
                    run_date=run_date,
                    key=key,
                    id="{:03}".format(id))
                )
                context.log.info(
                    f"Reached page count {page_count}, saving file at {path}")

                # If it's the first file, create directories and save path
                if id == 1:
                    paths_dict[key] = path
                    path.parent.mkdir(parents=True, exist_ok=True)

                # Save CSV file
                generate_df_and_save(contents, path)

                # Reset contents
                contents = None

        # Save last file
        if contents is not None:
            id += 1
            path = Path(path_template.render(
                run_date=run_date,
                key=key,
                id="{:03}".format(id))
            )
            if id == 1:
                paths_dict[key] = path
                path.parent.mkdir(parents=True, exist_ok=True)
            generate_df_and_save(contents, path)
            context.log.info(
                f"Saved last file with page count {page_count} at {path}")

        # Move on to the next endpoint.

    # Return paths
    return paths_dict


@solid(
    required_resource_keys={"basedosdados_config", "schedule_run_date"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def upload_to_bq(context, paths):
    for key in paths.keys():
        context.log.info("#" * 80)
        context.log.info(f"KEY = {key}")
        tb = Table(key, context.resources.basedosdados_config["dataset_id"],)
        tb_dir = paths[key].parent.parent
        context.log.info(f"tb_dir = {tb_dir}")

        if not tb.table_exists("staging"):
            context.log.info(
                "Table does not exist in STAGING, creating table...")
            tb.create(
                path=tb_dir,
                if_table_exists="pass",
                if_storage_data_exists="replace",
                if_table_config_exists="pass",
            )
            context.log.info("Table created in STAGING")
        else:
            context.log.info(
                "Table already exists in STAGING, appending to it...")
            tb.append(filepath=tb_dir, if_exists="replace",
                      timeout=600, chunk_size=1024*1024*10)
            context.log.info("Appended to table on STAGING successfully.")

        if not tb.table_exists("prod"):
            context.log.info("Table does not exist in PROD, publishing...")
            tb.publish(if_exists="pass")
            context.log.info("Published table in PROD successfully.")
        else:
            context.log.info("Table already published in PROD.")
    context.log.info(f"Returning -> {tb_dir.parent}")

    return tb_dir.parent


@solid
def cleanup_local(context, path):
    shutil.rmtree(path)


@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
            resource_defs={
                "basedosdados_config": basedosdados_config,
                "bd_client": bd_client,
                "schedule_run_date": schedule_run_date,
                "endpoints": endpoints,
            },
        )
    ],
    tags={
        "pipeline": "br_rj_riodejaneiro_sigmob_data",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "20m", "memory": "100Mi"},
                    "limits": {"cpu": "500m", "memory": "1Gi"},
                },
            }
        },
    },
)
def br_rj_riodejaneiro_sigmob_data():
    cleanup_local(
        upload_to_bq(
            request_data()
        )
    )
