import shutil
from typing import Any
import pandas as pd
import basedosdados as bd
import google.api_core.exceptions
import requests
from dagster import solid, pipeline, Output, ModeDefinition, OutputDefinition
from basedosdados import Table
from pathlib import Path
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client
from repositories.analises.resources import schedule_run_date


@solid(config_schema={"url": Any})
def get_routes(context):
    data = None

    try:
        data = requests.get(context.solid_config["url"])
    except Exception as e:
        raise e

    if data.ok:
        return data["result"]


@solid(required_resource_keys=["basedosdados_config", "schedule_run_date"])
def pre_treatment_br_rj_riodejaneiro_sigmob(context, data):
    data = data.json()
    run_date = context.resources.schedule_run_date["date"]
    path = f"{context.resources.basedosdados_config['table_id']}/data_versao={run_date}/routes_version_date={run_date}.csv"
    df = pd.DataFrame()
    df["route_id"] = [piece["route_id"] for piece in data]
    df["info"] = [piece for piece in data]
    df.to_csv(path, index=False)
    return Path(path)


@solid(required_resource_keys=["basedosdados_config", "schedule_run_date"])
def upload_to_bq(context, path):
    tb = bd.Table(
        context.resources.basedosdados_config["table_id"],
        context.resources.basedosdados_config["dataset_id"],
    )
    tb_dir = path.parent.parent

    if not tb.table_exists("staging"):
        tb.create(
            path=tb_dir,
            if_table_exists="pass",
            if_storage_data_exists="replace",
            if_table_config_exists="pass",
        )
    elif not tb.table_exists("prod"):
        tb.publish(if_exists="pass")
    else:
        tb.append(filepath=tb_dir, if_exists="replace")

    return shutil.rmtree(tb_dir.parent)


@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
            resource_defs={
                "basedosdados_config": basedosdados_config,
                "bd_client": bd_client,
                "schedule_run_date": schedule_run_date,
            },
        )
    ]
)
def br_rj_riodejaneiro_sigmob_routes():
    upload_to_bq(pre_treatment_br_rj_riodejaneiro_sigmob(get_routes()))
