import shutil
import pandas as pd
import basedosdados as bd
import google.api_core.exceptions
from dagster import solid, pipeline, ModeDefinition
from basedosdados.table import Table
from pathlib import Path
from datetime import datetime
from rgtfs import simple
from repositories.libraries.basedosdados.resources import basedosdados_config, bd_client
from repositories.analises.resources import schedule_run_date


def date_from_datetime(datetime_str):
    return datetime.strptime(datetime_str, "%Y-%m-%d").date()


def build_gtfs_version_name(versions, _date):

    _date = date_from_datetime(_date)

    valid_versions = [v for v in versions if v <= _date]

    version = (min(valid_versions, key=lambda x: abs(x - _date))).strftime("%Y%m%d")

    gtfs_partition = f"gtfs_version_date={version}"

    return gtfs_partition


@solid(
    config_schema={"dataset_id": str, "table_id": str, "storage_path": str},
    required_resource_keys={"schedule_run_date"},
)
def download_gtfs_from_storage(context):

    bucket = (
        bd.Storage(context.solid_config["dataset_id"], context.solid_config["table_id"])
        .client["storage_staging"]
        .bucket("rj-smtr-staging")
    )
    prefix = context.solid_config["storage_path"]
    blobs = [(blob.name, blob) for blob in bucket.list_blobs(prefix=prefix)]

    gtfs_versions = list(
        set(
            [
                datetime.strptime(blob[0].split("=")[1].split("/")[0], "%Y%m%d").date()
                for blob in blobs
            ]
        )
    )

    gtfs_partition = build_gtfs_version_name(
        gtfs_versions, context.resources.schedule_run_date["date"]
    )

    blob_obj = [blob[1] for blob in blobs if (prefix + gtfs_partition) in blob[0]]

    Path("tmp_data").mkdir(exist_ok=True)

    gtfs_path = f"tmp_data/{gtfs_partition}.zip"

    blob_obj[0].download_to_filename(filename=gtfs_path)

    return gtfs_path


@solid(
    config_schema={"query_table": str},
    required_resource_keys={"bd_client", "schedule_run_date"},
)
def get_daily_brt_gps_data(context, gtfs_path):

    run_date = context.resources.schedule_run_date["date"]

    query = f"""
    with brt_daily as (
    SELECT codigo AS vehicle_id, timestamp_gps AS datetime, latitude, longitude, linha
    FROM {context.solid_config["query_table"]} as t
    WHERE t.data =  DATE_SUB(DATE("{run_date}"), INTERVAL 1 DAY)
    OR (t.data = DATE_SUB(DATE("{run_date}"), INTERVAL 2 DAY) AND t.hora BETWEEN 20 AND 23)
    )
    SELECT * FROM brt_daily
    """

    gps_path = f"tmp_data/brt_daily_{date_from_datetime(run_date)}.csv"

    bd.download(
        savepath=gps_path,
        query=query,
        billing_project_id=context.resources.bd_client.project,
        from_file=True,
        index=False,
    )

    return {"gps_path": gps_path, "gtfs_path": gtfs_path}


def drop_overlap(df1, df2):
    df1 = df1.astype("string")
    df2 = df2.astype("string")

    _df = df1.merge(df2, on=df2.columns.to_list(), how="right", indicator=True)

    _df = _df[_df._merge == "right_only"]

    _df.drop("_merge", axis=1, inplace=True)

    return _df


def create_or_append_table(context, csv_path, which_table, _df, date):
    table_obj = Table(
        dataset_id=context.resources.basedosdados_config["dataset_id"],
        table_id=which_table,
    )
    query = f"""SELECT * FROM {table_obj.table_full_name['prod']} as t
            """
    if which_table == "realized_trips":
        query += f"""WHERE EXTRACT(DATE FROM t.departure_datetime) = DATE_SUB(DATE("{date}"), INTERVAL 1 DAY)"""
    if which_table == "unplanned":
        query += f"""WHERE DATE(t.dia) = DATE_SUB(DATE("{date}"), INTERVAL 1 DAY)"""

    try:
        ref = table_obj._get_table_obj("prod")
    except google.api_core.exceptions.NotFound:
        ref = None
    if ref:
        savepath = f"tmp_data/{which_table}_{date}_from_bq.csv"
        bd.download(
            savepath=savepath,
            query=query,
            billing_project_id=context.resources.bd_client.project,
            from_file=True,
            index=False,
        )

        tb = pd.read_csv(savepath, parse_dates=parse_dates)
        df = drop_overlap(tb, _df)
        df.to_csv(csv_path, index=False)

        table_obj.append(csv_path, if_exists="replace")
    else:
        _df.to_csv(csv_path, index=False)
        table_obj.create(
            csv_path, if_table_config_exists="pass", if_storage_data_exists="replace"
        )
        table_obj.publish(if_exists="replace")


@solid(
    required_resource_keys={"basedosdados_config", "bd_client", "schedule_run_date"},
)
def update_realized_trips(context, local_paths):
    date = date_from_datetime(context.resources.schedule_run_date["date"])
    local_paths["rt_filename"] = f"tmp_data/realized_trips_{date}.csv"
    local_paths["unplanned_filename"] = f"tmp_data/unplanned_{date}.csv"
    local_paths["rgtfs_path"] = f"tmp_data/rgtfs_{date}"

    realized_trips, unplanned = simple.main(
        local_paths["gtfs_path"],
        local_paths["gps_path"],
        local_paths["rgtfs_path"],
        stop_buffer_radius=100,
    )
    # Convert direction_id to int
    realized_trips["direction_id"] = realized_trips["direction_id"].astype("int64")

    create_or_append_table(
        context,
        csv_path=local_paths["rt_filename"],
        which_table="realized_trips",
        _df=realized_trips,
        date=context.resources.schedule_run_date["date"],
    )
    create_or_append_table(
        context,
        csv_path=local_paths["unplanned_filename"],
        which_table="unplanned",
        _df=unplanned,
        date=context.resources.schedule_run_date["date"],
    )

    for path in Path("tmp_data").glob(f"*_{date}*"):
        if str(path) == local_paths["rgtfs_path"]:
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)


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
def br_rj_riodejaneiro_brt_gtfs_gps_realized_trips():

    update_realized_trips(
        get_daily_brt_gps_data(download_gtfs_from_storage()),
    )
