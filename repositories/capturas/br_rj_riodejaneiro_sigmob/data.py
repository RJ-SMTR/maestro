from repositories.capturas.resources import endpoints
import shutil
import pandas as pd
import requests
from dagster import solid, pipeline, ModeDefinition, InputDefinition
from basedosdados import Table
from pathlib import Path
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client
from repositories.analises.resources import schedule_run_date


@solid(required_resource_keys={"endpoints"})
def request_data(context):
    data = None
    contents = {}
    endpoints = context.resources.endpoints["endpoints"]
    for key in endpoints.keys():
        try:
            data = requests.get(endpoints[key]["url"])
        except Exception as e:
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
                        data = requests.get(data.json()["next"])
                    except Exception as e:
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
    return paths


@solid(required_resource_keys={"basedosdados_config", "schedule_run_date"})
def upload_to_bq(context, paths):
    for key in paths.keys():
        tb = Table(key, context.resources.basedosdados_config["dataset_id"],)
        tb_dir = paths[key].parent.parent

        if not tb.table_exists("staging"):
            tb.create(
                path=tb_dir,
                if_table_exists="pass",
                if_storage_data_exists="replace",
                if_table_config_exists="pass",
            )
        else:
            tb.append(filepath=tb_dir, if_exists="replace")

        if not tb.table_exists("prod"):
            tb.publish(if_exists="pass")

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
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "250m", "memory": "500Mi"},
                    "limits": {"cpu": "1500m", "memory": "1Gi"},
                },
            }
        },
    },
)
def br_rj_riodejaneiro_sigmob_data():
    cleanup_local(upload_to_bq(pre_treatment_br_rj_riodejaneiro_sigmob(request_data())))
