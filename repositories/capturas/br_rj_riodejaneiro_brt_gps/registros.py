from dagster import (
    solid,
    pipeline,
    resource,
    Output,
    OutputDefinition,
    InputDefinition,
    ModeDefinition,
    PresetDefinition,
    Field,
)
import basedosdados as bd

import requests
import json
import pendulum
import pandas as pd
from pathlib import Path
import os


@resource(
    {
        "table_id": Field(
            str, is_required=True, description="Table used in the pipeline"
        ),
        "dataset_id": Field(
            str, is_required=True, description="Dataset used in the pipeline"
        ),
    }
)
def basedosdados_config(context):
    return context.resource_config

@resource(
    {
        "timezone": Field(
            str, is_required=True, description="Run timezone"
        ),
    }
)
def timezone_config(context):
    return context.resource_config


@solid(
    output_defs=[
        OutputDefinition(name="file_path"),
        OutputDefinition(name="partitions"),
    ],
    required_resource_keys={"basedosdados_config", "timezone_config"},
)
def get_file_path_and_partitions(context):

    table_id = context.resources.basedosdados_config['table_id']
    dataset_id = context.resources.basedosdados_config['dataset_id']
    timezone = context.resources.timezone_config["timezone"]

    capture_time = pendulum.now(timezone)
    date = capture_time.strftime("%Y-%m-%d")
    hour = capture_time.strftime("%H")
    filename = capture_time.strftime("%Y-%m-%d-%H-%M-%S")

    partitions = f"data={date}/hora={hour}"

    file_path = f"{os.getcwd()}/data/{{mode}}/{dataset_id}/{table_id}/{partitions}/{filename}.{{filetype}}"
    context.log.info(f"creating file path {file_path}")

    yield Output(file_path, output_name="file_path")
    yield Output(partitions, output_name="partitions")


@solid
def get_raw(context, url):

    data = requests.get(url)

    if data.ok:
        return data
    else:
        raise Exception(f"Requests failed with error {data.status_code}")


@solid(
    required_resource_keys={"basedosdados_config", "timezone_config"},
)
def pre_treatment(context, data):

    timezone = context.resources.timezone_config["timezone"]

    data = data.json()
    df = pd.DataFrame(data["veiculos"])
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    df["dataHora"] = df["dataHora"].apply(
        lambda ms: pd.to_datetime(pendulum.from_timestamp(ms / 1000.0, timezone).isoformat())
    )

    return df


@solid
def save_raw_local(context, data, file_path, mode="raw"):

    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    json.dump(data.json(), Path(_file_path).open("w"))

    return _file_path


@solid
def save_treated_local(context, df, file_path, mode="staging"):

    _file_path = file_path.format(mode=mode, filetype="csv")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(_file_path, index=False)

    return _file_path


@solid(required_resource_keys={"basedosdados_config"})
def upload_to_bigquery(context, treated_file_path, raw_file_path, partitions):

    table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    st.upload(raw_file_path, partitions=partitions, mode="raw")
    st.upload(treated_file_path, partitions=partitions, mode="staging")

    delete_file(raw_file_path)
    delete_file(treated_file_path)


@solid(required_resource_keys={"basedosdados_config"})
def create_table_bq(context, file_path):

    table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    tb.create(
        path=Path(file_path).parent.parent.parent,
        partitioned=True,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="pass",
    )

    tb.publish(if_exists="replace")


def delete_file(file):

    return Path(file).unlink()


@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config, "timezone_config": timezone_config}
        ),
    ]
)
def br_rj_riodejaneiro_brt_gps_registros():

    file_path, partitions = get_file_path_and_partitions()

    data = get_raw()

    raw_file_path = save_raw_local(data, file_path)

    treated_data = pre_treatment(data)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery(treated_file_path, raw_file_path, partitions)


@pipeline(
    preset_defs=[
        PresetDefinition.from_files(
            "init",
            config_files=[str(Path(__file__).parent / "registros.yaml")],
            mode="dev",
        )
    ],
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config, "timezone_config": timezone_config}
        ),
    ],
)
def br_rj_riodejaneiro_brt_gps_registros_init():

    file_path, partitions = get_file_path_and_partitions()

    data = get_raw()

    raw_file_path = save_raw_local(data, file_path)

    treated_data = pre_treatment(data)

    treated_file_path = save_treated_local(treated_data, file_path)

    create_table_bq(treated_file_path)