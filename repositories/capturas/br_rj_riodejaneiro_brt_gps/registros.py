from dagster import (
    solid,
    pipeline,
    Output,
    OutputDefinition,
    InputDefinition,
    ModeDefinition,
    PresetDefinition,
)
import basedosdados as bd

import requests
import json
import datetime
import pandas as pd
from pathlib import Path
import os
import os


@solid(
    output_defs=[
        OutputDefinition(name="file_path"),
        OutputDefinition(name="partitions"),
    ],
)
def get_file_path_and_partitions(context, dataset_id, table_id):

    capture_time = datetime.datetime.now()
    date = capture_time.strftime("%Y-%m-%d")
    hour = capture_time.strftime("%H")
    filename = capture_time.strftime("%Y-%m-%d-%H-%m-%S")

    partitions = f"data={date}/hora={hour}"

    file_path = f"{os.getcwd()}/data/{{mode}}/{dataset_id}/{table_id}/{partitions}/{filename}.{{filetype}}"

    yield Output(file_path, output_name="file_path")
    yield Output(partitions, output_name="partitions")


@solid
def get_raw(context, url):

    data = requests.get(url)

    if data.ok:
        return data
    else:
        raise Exception("Requests failed with error {data.status_code}")


@solid
def pre_treatment(context, data):

    data = data.json()
    df = pd.DataFrame(data["veiculos"])
    df["timestamp_captura"] = datetime.datetime.now()
    df["dataHora"] = df["dataHora"].apply(
        lambda ms: datetime.datetime.fromtimestamp(ms / 1000.0)
    )

    return df


@solid
def save_raw_local(context, data, file_path, mode="raw"):

    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    json.dump(data.json(), Path(_file_path).open("w"))


@solid
def save_treated_local(context, df, file_path, mode="staging"):

    _file_path = file_path.format(mode=mode, filetype="csv")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(_file_path, index=False)

    return _file_path


@solid
def upload_to_bigquery(
    context, file_path, partitions, dataset_id, table_id, mode="staging"
):

    _file_path = file_path.format(mode=mode, filetype="csv")

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    st.upload(_file_path, partitions=partitions, mode="staging")

    delete_file(_file_path)


def delete_file(file):

    return Path(file).unlink()


local_mode = ModeDefinition(name="local")


@pipeline(
    # ordered so the local is first and therefore the default
    mode_defs=[local_mode]
)
def br_rj_riodejaneiro_brt_gps_registros():

    file_path, partitions = get_file_path_and_partitions()

    data = get_raw()

    save_raw_local(data, file_path)

    treated_data = pre_treatment(data)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery(treated_file_path, partitions)
