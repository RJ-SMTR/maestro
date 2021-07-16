import shutil
import pandas as pd
import basedosdados as bd
import google.api_core.exceptions
from dagster import solid, pipeline, ModeDefinition
from basedosdados import Table
from pathlib import Path
from datetime import datetime, date, timedelta
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client
from repositories.analises.resources import schedule_run_date


@solid(
    config_schema={"query_tables": list},
    required_resource_keys={"basedosdados_config", "bd_client", "schedule_run_date"},
)
def get_daily_data(context):
    run_date = (
        datetime.strptime(context.resources.schedule_run_date["date"], "%Y-%m-%d")
        - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    dataset_id = context.resources.basedosdados_config["dataset_id"]
    project = context.resources.bd_client.project
    paths = {}

    for table_id in context.solid_config["query_tables"]:
        query = f"""
        SELECT *
        FROM {project}.{dataset_id}.{table_id} as t
        WHERE t.data = "{run_date}"
        """
        path = Path(f"{run_date}/{table_id}/data={run_date}/{table_id}_daily.csv")
        try:
            bd.download(
                savepath=path,
                query=query,
                billing_project_id=project,
                from_file=True,
                index=False,
            )
            paths[table_id] = path
        except Exception as e:
            raise Exception(f"unable to download {table_id}: {e}") from e
    return paths


def drop_csv_columns(path, columns):
    df = pd.read_csv(path)
    df.drop(columns, axis=1, inplace=True)
    df.to_csv(path, index=False)


@solid(
    config_schema={"query_tables": list},
    required_resource_keys={"basedosdados_config", "bd_client", "schedule_run_date"},
)
def upload_table(context, paths):

    for table_id in context.solid_config["query_tables"]:
        tb = bd.Table(
            dataset_id=context.resources.basedosdados_config["dataset_id"],
            table_id=table_id,
        )
        tb_dir = paths[table_id].parent.parent

        drop_csv_columns(paths[table_id], columns="data")

        if not tb.table_exists("staging"):
            tb.create(
                path=tb_dir,
                if_table_exists="replace",
                if_storage_data_exists="replace",
                if_table_config_exists="pass",
            )
        if not tb.table_exists("prod"):
            tb.publish(if_exists="replace")
        else:
            tb.append(filepath=tb_dir, if_exists="replace")

    # Delete local files
    shutil.rmtree(tb_dir.parent)


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
def projeto_multa_automatica_multas_aplicaveis_onibus():
    upload_table(get_daily_data())
