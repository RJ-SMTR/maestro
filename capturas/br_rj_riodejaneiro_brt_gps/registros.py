import basedosdados as bd

import requests
import json
import datetime
import pandas as pd
from pathlib import Path
import os
import os


def get_file_path_and_partitions(dataset_id, table_id):

    capture_time = datetime.datetime.now()
    date = capture_time.strftime("%Y-%m-%d")
    hour = capture_time.strftime("%H")
    filename = capture_time.strftime("%Y-%m-%d-%H-%m-%S")

    partitions = f"data={date}/hora={hour}"

    file_path = f"{os.getcwd()}/data/{{mode}}/{dataset_id}/{table_id}/{partitions}/{filename}.{{filetype}}"

    yield Output(file_path, output_name="file_path")
    yield Output(partitions, output_name="partitions")


def get_raw(url):

    data = requests.get(url)

    if data.ok:
        return data
    else:
        raise Exception("Requests failed with error {data.status_code}")


def pre_treatment(data):

    data = data.json()
    df = pd.DataFrame(data["veiculos"])
    df["timestamp_captura"] = datetime.datetime.now()
    df["dataHora"] = df["dataHora"].apply(
        lambda ms: datetime.datetime.fromtimestamp(ms / 1000.0)
    )

    return df


def save_raw_local(data, file_path, mode="raw"):

    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    json.dump(data.json(), Path(_file_path).open("w"))


def save_treated_local(df, file_path, mode="staging"):

    _file_path = file_path.format(mode=mode, filetype="csv")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(_file_path, index=False)

    return _file_path


def upload_to_bigquery(file_path, partitions, dataset_id, table_id, mode="staging"):

    _file_path = file_path.format(mode=mode, filetype="csv")

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    st.upload(_file_path, partitions=partitions, mode="staging")

    delete_file(_file_path)


def delete_file(file):

    return Path(file).unlink()


def entrypoint():

    file_path, partitions = get_file_path_and_partitions()

    data = get_raw()

    save_raw_local(data, file_path)

    treated_data = pre_treatment(data)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery(treated_file_path, partitions)
