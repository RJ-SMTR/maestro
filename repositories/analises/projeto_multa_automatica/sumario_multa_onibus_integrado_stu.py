from os import replace
import shutil
import basedosdados as bd
from dagster import solid, pipeline, ModeDefinition
from basedosdados import Storage
from pathlib import Path
from datetime import datetime, timedelta
from repositories.libraries.basedosdados.resources import bd_client
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
    run_date = (
        datetime.strptime(context.resources.schedule_run_date["date"], "%Y-%m-%d")
        - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    query = f"""
        SELECT *
        FROM {context.solid_config.query_table}
        WHERE data = '{run_date}'
    """
    context.log.info(f"Running query\n {query}")

    filename = f"{run_date}/multas{run_date.replace('-','')}.csv"

    context.log.info(f"Downloading query results and saving as {filename}")

    bd.download(
        savepath=filename,
        query=query,
        billing_project_id=project,
        from_file=True,
        index=False,
        sep=";",
    )
    return filename


@solid(
    config_schema={"dataset_id": str, "table_id": str},
    required_resource_keys={"bd_client"},
)
def upload(context, filename):
    st = Storage(
        dataset_id=context.solid_config["dataset_id"],
        table_id=context.solid_config["table_id"],
    )
    context.log.info(
        f"Uploading {filename} to GCS at {context.resources.bd_client.project}/staging/{context.solid_config['dataset_id']}/{context.solid_config['table_id']}"
    )
    st.upload(path=filename, mode="staging", if_exists=replace)

    return filename


@solid()
def cleanup(context, filename):
    context.log.info(f"Starting cleanup, deleting {filename} from local")
    return shutil.rmtree(Path(filename).parent)


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
