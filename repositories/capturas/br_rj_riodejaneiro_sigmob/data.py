import shutil
import traceback
from pathlib import Path

import requests
import pandas as pd
from basedosdados import Table
from dagster import solid, pipeline, ModeDefinition

from repositories.helpers.hooks import log_critical
from repositories.helpers.constants import constants
from repositories.capturas.resources import endpoints
from repositories.analises.resources import schedule_run_date
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client


@solid(required_resource_keys={"endpoints"})
def request_data(context):
    data = None
    contents = {}
    endpoints = context.resources.endpoints["endpoints"]
    for key in endpoints.keys():
        context.log.info("#" * 80)
        context.log.info(f"KEY = {key}")
        try:
            context.log.info(f"URL = {endpoints[key]['url']}")
            data = requests.get(
                endpoints[key]["url"],
                timeout=constants.SIGMOB_GET_REQUESTS_TIMEOUT.value,
            )
        except Exception as e:
            err = traceback.format_exc()
            log_critical(f"Failed to request data from SIGMOB: \n{err}")
            raise e
        if data.ok:
            if "next" in data.json().keys():
                while data.json()["next"] != "EOF":
                    if key not in contents.keys():
                        contents[key] = {
                            "data": data.json()["data"],
                            "key_column": endpoints[key]["key_column"],
                        }
                    else:
                        contents[key]["data"].extend(data.json()["data"])
                    try:
                        context.log.info(f"URL = {data.json()['next']}")
                        data = requests.get(data.json()["next"])
                    except Exception as e:
                        err = traceback.format_exc()
                        log_critical(
                            f"Failed to request data from SIGMOB: \n{err}")
                        raise e
            else:
                contents[key] = {
                    "data": data.json()["result"],
                    "key_column": endpoints[key]["key_column"],
                }
    return contents


@solid(required_resource_keys={"basedosdados_config", "schedule_run_date"},)
def pre_treatment_br_rj_riodejaneiro_sigmob(context, contents):
    run_date = context.resources.schedule_run_date["date"]
    paths = {}

    for key in contents.keys():
        context.log.info("#" * 80)
        context.log.info(f"KEY = {key}")
        path = Path(
            f"{run_date}/{key}/data_versao={run_date}/{key}_version-{run_date}.csv"
        )

        df = pd.DataFrame()
        df[contents[key]["key_column"]] = [
            piece[contents[key]["key_column"]] for piece in contents[key]["data"]
        ]
        df["content"] = [piece for piece in contents[key]["data"]]

        path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(path, index=False)

        paths[key] = path
        context.log.info(f"PATH = {path}")
    return paths


@solid(required_resource_keys={"basedosdados_config", "schedule_run_date"})
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
            tb.append(filepath=tb_dir, if_exists="replace", timeout=600)
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
                    "requests": {"cpu": "20m", "memory": "800Mi"},
                    "limits": {"cpu": "500m", "memory": "2Gi"},
                },
            }
        },
    },
)
def br_rj_riodejaneiro_sigmob_data():
    cleanup_local(
        upload_to_bq(
            pre_treatment_br_rj_riodejaneiro_sigmob(
                request_data()
            )
        )
    )
