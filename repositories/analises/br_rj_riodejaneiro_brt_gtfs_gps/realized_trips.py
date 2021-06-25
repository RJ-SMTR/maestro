from typing import Any
from dagster import solid, pipeline, ModeDefinition, Partition, PartitionSetDefinition
import pandas as pd
import basedosdados as bd
import google.api_core.exceptions
from basedosdados.table import Table
from datetime import datetime
from rgtfs import simple


def get_gtfs_versions():
    prefix = "raw/br_rj_riodejaneiro_gtfs_planned/gtfs_planned"
    st = bd.Storage("br_rj_riodejaneiro_gtfs_planned", "gtfs_planned")
    blobs = [
        blob.name
        for blob in st.client["storage_staging"]
        .bucket("rj-smtr-staging")
        .list_blobs(prefix=prefix)
    ]
    versions = list(
        set(
            [
                datetime.strptime(blob.split("=")[1].split("/")[0], "%Y%m%d").date()
                for blob in blobs
            ]
        )
    )
    return versions


def build_gtfs_version_name(versions, _date):

    _date = datetime.strptime(_date, "%Y-%m-%d").date()

    valid_versions = [v for v in versions if v <= _date]

    version = (min(valid_versions, key=lambda x: abs(x - _date))).strftime("%Y%m%d")

    gtfs_blob_name = f"gtfs_version_date={version}"

    return gtfs_blob_name


@solid(config_schema={"date": str})
def download_gtfs_from_storage(context):

    bucket = (
        bd.Storage("br_rj_riodejaneiro_gtfs_planned", "gtfs_planned")
        .client["storage_staging"]
        .bucket("rj-smtr-staging")
    )
    prefix = "raw/br_rj_riodejaneiro_gtfs_planned/gtfs_planned/"

    gtfs_partition = build_gtfs_version_name(
        get_gtfs_versions(), context.solid_config["date"]
    )

    blob_obj = [blob for blob in bucket.list_blobs(prefix=(prefix + gtfs_partition))]
    gtfs_path = f"{gtfs_partition}.zip"

    blob_obj[0].download_to_filename(filename=gtfs_path)

    return True


@solid(config_schema={"date": str})
def get_daily_brt_gps_data(context, gtfs_data):

    query = f"""
    with brt_daily as (
    SELECT codigo AS vehicle_id, timestamp_gps AS datetime, latitude, longitude, linha
    FROM rj-smtr.dashboard_monitoramento_brt.registros_tratada as t
    WHERE t.data =  DATE_SUB(DATE("{context.solid_config['date']}"), INTERVAL 1 DAY)
    OR (t.data = DATE_SUB(DATE("{context.solid_config['date']}"), INTERVAL 2 DAY) AND t.hora BETWEEN 20 AND 23)
    )
    SELECT * FROM brt_daily
    """

    gps_path = f"brt_daily_{context.solid_config['date']}.csv"

    bd.download(
        savepath=gps_path,
        query=query,
        billing_project_id="rj-smtr-dev",  #### TODO: mudar no deploy
        from_file=True,
        index=False,
    )

    return pd.read_csv(gps_path)


def drop_overlap(df1, df2):
    df2 = df2.astype("str")

    _df = df1.merge(df2, on=df2.columns.to_list(), how="right", indicator=True)

    _df = _df[_df._merge == "right_only"]

    _df.drop("_merge", axis=1, inplace=True)

    return _df


@solid(config_schema={"date": str})
def update_realized_trips(context, gps_data):
    date = datetime.strptime(context.solid_config["date"], "%Y-%m-%d").date()
    rt_filename = f"realized_trips_{date}.csv"
    unplanned_filename = f"unplanned_{date}.csv"
    rgtfs_path = f"rgtfs_{date}"
    gps_path = f"brt_daily_{date}.csv"

    gtfs_partition = build_gtfs_version_name(
        get_gtfs_versions(), context.solid_config["date"]
    )

    gtfs_path = f"{gtfs_partition}.zip"

    RT, unplanned = simple.main(
        gtfs_path,
        gps_path,
        rgtfs_path,
        stop_buffer_radius=100,
    )

    unplanned.to_csv(unplanned_filename, index=False)

    RT["direction_id"] = RT["direction_id"].astype("int64")

    realized = Table(
        dataset_id="br_rj_riodejaneiro_brt_gtfs_gps", table_id="realized_trips"
    )

    try:
        ref = realized._get_table_obj("staging")
    except google.api_core.exceptions.NotFound:
        ref = None

    if ref:
        tb = bd.read_table(
            "br_rj_riodejaneiro_brt_gtfs_gps_staging",
            "realized_trips",
            query_project_id="rj-smtr-dev",  #### TODO: mudar no deploy
            billing_project_id="rj-smtr-dev",  #### TODO: mudar no deploy
            from_file=True,
        )

        df = drop_overlap(tb, RT)

        df.to_csv(rt_filename, index=False)

        realized.append(filepath=rt_filename, if_exists="replace")
    else:
        RT.to_csv(rt_filename, index=False)

        realized.create(
            path=rt_filename,
            if_table_config_exists="replace",
            if_table_exists="replace",
            if_storage_data_exists="replace",
        )

    realized.publish(if_exists="replace")


@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
        )
    ]
)
def br_rj_riodejaneiro_brt_gtfs_gps_realized_trips():

    update_realized_trips(
        get_daily_brt_gps_data(download_gtfs_from_storage()),
    )
