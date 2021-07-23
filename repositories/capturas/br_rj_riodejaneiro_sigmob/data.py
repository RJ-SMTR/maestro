import shutil
import pandas as pd
import requests
from dagster import solid, pipeline, ModeDefinition, InputDefinition
from basedosdados import Table
from pathlib import Path
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client
from repositories.analises.resources import schedule_run_date


@solid(input_defs=[InputDefinition(name="api_keys", dagster_type=list)])
def request_data(context, api_keys):
    data = None
    contents = {}
    for url in api_keys:
        key = url.split("get_")[1].split(".")[0]
        try:
            data = requests.get(url)
        except Exception as e:
            raise e
        if data.ok:
            contents[key] = data.json()["result"]
    return contents


@solid(required_resource_keys={"basedosdados_config", "schedule_run_date"})
def pre_treatment_br_rj_riodejaneiro_sigmob(context, contents):
    run_date = context.resources.schedule_run_date["date"]
    paths = {}
    for key in contents.keys():
        path = Path(
            f"{run_date}/{key}/data_versao={run_date}/{key}_version-{run_date}.csv"
        )
        df = pd.DataFrame()
        if key == "routes":
            df["route_id"] = [piece["route_id"] for piece in contents[key]]
        if key == "linhas":
            df["linha_id"] = [piece["linha_id"] for piece in contents[key]]

        df["content"] = [piece for piece in contents[key]]
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        paths[key] = path
    return paths


@solid(required_resource_keys={"basedosdados_config", "schedule_run_date"})
def upload_to_bq(context, paths):
    for key in paths.keys():
        tb = Table(
            key,
            context.resources.basedosdados_config["dataset_id"],
        )
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
            },
        )
    ]
)
def br_rj_riodejaneiro_sigmob_data():
    cleanup_local(upload_to_bq(pre_treatment_br_rj_riodejaneiro_sigmob(request_data())))
