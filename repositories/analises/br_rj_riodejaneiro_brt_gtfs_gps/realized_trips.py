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
from repositories.analises.resources import gtfs, gps_data


def build_gtfs_version_name(versions, _date):

    _date = datetime.strptime(_date, "%Y-%m-%d").date()

    valid_versions = [v for v in versions if v <= _date]

    version = (min(valid_versions, key=lambda x: abs(x - _date))).strftime("%Y%m%d")

    gtfs_partition = f"gtfs_version_date={version}"

    return gtfs_partition


@solid(
    config_schema={"date": str}, required_resource_keys={"basedosdados_config", "gtfs"}
)
def download_gtfs_from_storage(context):

    bucket = (
        bd.Storage(
            context.resources.gtfs["dataset_id"], context.resources.gtfs["table_id"]
        )
        .client["storage_staging"]
        .bucket("rj-smtr-staging")
    )
    prefix = context.resources.gtfs["storage_path"]

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
        gtfs_versions, context.solid_config["date"]
    )

    blob_obj = [blob[1] for blob in blobs if (prefix + gtfs_partition) in blob[0]]

    Path("tmp_data").mkdir(exist_ok=True)

    gtfs_path = f"tmp_data/{gtfs_partition}.zip"

    blob_obj[0].download_to_filename(filename=gtfs_path)

    return gtfs_path


@solid(config_schema={"date": str}, required_resource_keys={"bd_client", "gps_data"})
def get_daily_brt_gps_data(context, gtfs_path):

    query = f"""
    with brt_daily as (
    SELECT codigo AS vehicle_id, timestamp_gps AS datetime, latitude, longitude, linha
    FROM {context.resources.gps_data["query_table"]} as t
    WHERE t.data =  DATE_SUB(DATE("{context.solid_config['date']}"), INTERVAL 1 DAY)
    OR (t.data = DATE_SUB(DATE("{context.solid_config['date']}"), INTERVAL 2 DAY) AND t.hora BETWEEN 20 AND 23)
    )
    SELECT * FROM brt_daily
    """

    gps_path = f"tmp_data/brt_daily_{context.solid_config['date']}.csv"

    bd.download(
        savepath=gps_path,
        query=query,
        billing_project_id=context.resources.bd_client.project,
        from_file=True,
        index=False,
    )

    return {"gps_path": gps_path, "gtfs_path": gtfs_path}


def drop_overlap(df1, df2):

    for col in df1.columns:
        df2[col] = df2[col].astype(df1[col].dtypes.name)

    _df = df1.merge(df2, on=df2.columns.to_list(), how="right", indicator=True)

    _df = _df[_df._merge == "right_only"]

    _df.drop("_merge", axis=1, inplace=True)

    return _df


def create_or_append_table(table_obj, csv_path, which_table, _df, date, project_id):
    try:
        ref = table_obj._get_table_obj("prod")
    except google.api_core.exceptions.NotFound:
        ref = None
    query = f"""SELECT * FROM {table_obj.table_full_name['prod']} as t
            """

    if which_table == "realized_trips":
        query += f"""WHERE EXTRACT(DATE FROM t.departure_datetime) = DATE_SUB(DATE("{date}"), INTERVAL 1 DAY)"""
    if which_table == "unplanned":
        query += f"""WHERE DATE(t.dia) = DATE_SUB(DATE("{date}"), INTERVAL 1 DAY)"""

    if ref:
        bd.download(
            savepath=f"{which_table}_{date}_from_bq.csv",
            query=query,
            billing_project_id=project_id,
            from_file=True,
            index=False,
        )
        tb = pd.read_csv(f"{which_table}_{date}_from_bq.csv")
        if which_table == "unplanned":
            _df["n_registros"] = _df["n_registros"].astype("int64")
        if which_table == "realized_trips":
            _df["direction_id"] = _df["direction_id"].astype("int64")

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
    config_schema={"date": str},
    required_resource_keys={"basedosdados_config", "bd_client"},
)
def update_realized_trips(context, local_paths):
    date = datetime.strptime(context.solid_config["date"], "%Y-%m-%d").date()
    rt_filename = f"tmp_data/realized_trips_{date}.csv"
    unplanned_filename = f"tmp_data/unplanned_{date}.csv"
    rgtfs_path = f"tmp_data/rgtfs_{date}"

    gps_path = local_paths["gps_path"]

    gtfs_path = local_paths["gtfs_path"]

    realized_trips, unplanned = simple.main(
        gtfs_path,
        gps_path,
        rgtfs_path,
        stop_buffer_radius=100,
    )

    realized_tb = Table(
        dataset_id=context.resources.basedosdados_config["dataset_id"],
        table_id="realized_trips",
    )
    unplanned_tb = Table(
        dataset_id=context.resources.basedosdados_config["dataset_id"],
        table_id="unplanned",
    )

    create_or_append_table(
        table_obj=realized_tb,
        csv_path=rt_filename,
        which_table="realized_trips",
        _df=realized_trips,
        date=context.solid_config["date"],
        project_id=context.resources.bd_client.project,
    )
    create_or_append_table(
        table_obj=unplanned_tb,
        csv_path=unplanned_filename,
        which_table="unplanned",
        _df=unplanned,
        date=context.solid_config["date"],
        project_id=context.resources.bd_client.project,
    )

    for path in Path("tmp_data").glob(f"*_{date}"):
        if str(path) == rgtfs_path:
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
                "gtfs": gtfs,
                "gps_data": gps_data,
            },
        )
    ]
)
def br_rj_riodejaneiro_brt_gtfs_gps_realized_trips():

    update_realized_trips(
        get_daily_brt_gps_data(download_gtfs_from_storage()),
    )
