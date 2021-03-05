from dagster import (
    solid,
    Output,
    OutputDefinition,
)

import requests
import json
import pendulum
import pandas as pd
from pathlib import Path
import os

import basedosdados as bd


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
        if_storage_data_exists="pass",
        if_table_config_exists="pass",
    )

    tb.publish(if_exists="replace")


def delete_file(file):

    return Path(file).unlink()