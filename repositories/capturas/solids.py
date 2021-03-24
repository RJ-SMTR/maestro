from dagster import (
    solid,
    Output,
    OutputDefinition,
    composite_solid,
)

import requests
import json
import pendulum
import pandas as pd
from pathlib import Path
import os
from openpyxl import load_workbook
import re

import basedosdados as bd
# Temporario, essa funcao vai ser incorporada a base dos dados
from repositories.helpers.storage import StoragePlus


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


@solid(
    output_defs=[
        OutputDefinition(name="filename"),
        OutputDefinition(name="filetype"),
        OutputDefinition(name="file_path"),
        OutputDefinition(name="partitions"),
    ],
)
def parse_file_path_and_partitions(context, bucket_path):

    # Parse bucket to get mode, dataset_id, table_id and filename
    path_list = bucket_path.split('/')
    dataset_id = path_list[1]
    table_id = path_list[2]
    filename = path_list[-1].split(".")[0]
    filetype = path_list[-1].split(".")[1]

    # Parse bucket to get partitions
    partitions = re.findall("\/([^\/]*?)=(.*?)(?=\/)", bucket_path)
    partitions = "/".join(["=".join([field for field in item]) for item in partitions])
        
    file_path = f"{os.getcwd()}/data/{{mode}}/{dataset_id}/{table_id}/{partitions}/{filename}.{{filetype}}"
    context.log.info(f"creating file path {file_path}")

    yield Output(filename, output_name="filename")
    yield Output(filetype, output_name="filetype")
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
def upload_to_bigquery(context, file_paths, partitions, modes=['raw', 'staging']):

    table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    for idx, mode in enumerate(modes):
        context.log.info(f"Uploading to mode {mode}")
        st.upload(file_paths[idx], partitions=partitions, mode=mode, if_exists='replace')
        delete_file(file_paths[idx])


@solid(required_resource_keys={"basedosdados_config"})
def create_table_bq(context, file_path, table_config='replace', publish_config='pass'):

    table_id = context.resources.basedosdados_config["table_id"]
    dataset_id = context.resources.basedosdados_config["dataset_id"]

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    tb.create(
        path=Path(file_path).parent.parent.parent,
        partitioned=True,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists=table_config,
    )

    tb.publish(if_exists=publish_config)


def delete_file(file):
    return Path(file).unlink(missing_ok=True)


@solid(
    required_resource_keys={"basedosdados_config"},
)
def get_file_from_storage(context, file_path, filename, partitions, mode='raw', filetype="xlsx"):

    # Download from storage
    table_id = context.resources.basedosdados_config['table_id']
    dataset_id = context.resources.basedosdados_config['dataset_id']

    _file_path = file_path.format(mode=mode, filetype=filetype)

    st = StoragePlus(table_id=table_id, dataset_id=dataset_id)
    context.log.debug(f"File path: {_file_path}")
    context.log.debug(f"filename: {filename}")
    context.log.debug(f"partition: {partitions}")
    context.log.debug(f"mode: {mode}")
    st.download(filename=filename+"."+filetype, file_path=_file_path, partitions=partitions, mode=mode,
                if_exists='replace')
    return _file_path


@solid
def delete_xls_header(context, file_path):
    wb = load_workbook(file_path)
    ws =  wb.active
    # Delete current header
    ws.delete_rows(1)
    wb.save(file_path)
    return file_path


@solid
def set_xls_header(context, file_path, header):
    wb = load_workbook(file_path)
    ws =  wb.active
    ws.insert_rows(1)
    for idx, column in enumerate(header):
        ws.cell(row=1, column=idx+1).value = column
    wb.save(file_path)

    return file_path

@composite_solid
def set_header(file_path, header):
    file_path = delete_xls_header(file_path)
    file_path = set_xls_header(file_path, header)
    return file_path