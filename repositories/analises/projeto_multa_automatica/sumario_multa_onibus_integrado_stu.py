import shutil
import pandas as pd
import basedosdados as bd
from dagster import solid, pipeline, ModeDefinition
from basedosdados import Table
from pathlib import Path
from datetime import datetime, timedelta
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client
from repositories.analises.resources import schedule_run_date


@solid(
    config_schema={"query_table": str, "date_format": str},
    required_resource_keys={"bd_client", "schedule_run_date"},
)
def query_data(context):
    project = context.resources.bd_client.project
    context.log.info(
        f"Fetching data from {project}.{context.solid_config['query_table']}"
    )
    query = f"""
        SELECT *
        FROM {project}.{context.solid_config['query_table']}
    """
    run_date = datetime.strptime(
        context.resources.schedule_run_date["date"], "%Y-%m-%d"
    ).strftime(f"{context.solid_config['date_format']}")

    filename = f"{run_date}/multas{run_date}.csv"

    context.log.info(
        f"Downloading query results and saving as {run_date}/multas{run_date}.csv"
    )
    bd.download(
        savepath=filename, query=query, billing_project_id=project, index=False, sep=";"
    )
    return filename


@solid(config_schema={"dataset_id": str, "table_id": str})
def upload(context, filename):
    tb = Table(
        table_id=context.solid_config["table_id"],
        dataset_id=context.solid_config["dataset_id"],
    )
    if not tb.table_exists("staging"):
        context.log.info(
            f"Table does not exist at STAGING, creating table {context.solid_config['dataset_id']}.{context.solid_config['table_id']}"
        )
        tb.create(
            path=filename, if_table_config_exists="pass", if_storage_data_exists="pass"
        )
    elif not tb.table_exists("prod"):
        context.log.info(
            f"Table does not exist at PROD, creating view {context.solid_config['dataset_id']}.{context.solid_config['table_id']}"
        )
        tb.publish()
    else:
        context.log.info(
            f"Table already exists, appending to table {context.solid_config['dataset_id']}.{context.solid_config['table_id']}"
        )
        tb.append(filename)

    return filename


@solid()
def cleanup(context, filename):
    context.log.info(f"Starting cleanup, deleting {filename} from local")
    return shutil.rmtree(filename)


@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
            resource_defs={
                "bd_client": bd_client,
                "schedule_run_date": schedule_run_date,
            },
        )
    ],
    tags={
        "pipeline": "projeto_multa_automatica_sumario_integrado_stu",
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
def projeto_multa_automatica_sumario_multa_onibus_integrado_stu():
    cleanup(upload(query_data()))
