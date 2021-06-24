from dagster import solid, pipeline, ModeDefinition, InputDefinition
from pandas.core.dtypes import dtypes
import pandas as pd
import basedosdados as bd
import google.api_core.exceptions
from basedosdados.table import Table
from datetime import date
from rgtfs import simple


@solid
def get_daily_brt_gps_data(
    context,
):
    query = """
    with brt_daily as (
    SELECT codigo AS vehicle_id, timestamp_gps AS datetime, latitude, longitude, linha
    FROM rj-smtr.dashboard_monitoramento_brt.registros_tratada as t
    WHERE t.data =  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    OR (t.data = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND t.hora BETWEEN 20 AND 23)
    )
    SELECT * FROM brt_daily
    """
    # query = """SELECT ordem AS vehicle_id, latitude, longitude, timestamp_gps AS datetime
    #             FROM rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada as t
    #             WHERE t.data =  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    #             OR (t.data = DATE_SUB(CURRENT_DATE, INTERVAL 2 DAY) AND t.hora BETWEEN 20 AND 23)
    #             LIMIT 1000
    #         """

    gps_path = "brt_daily.csv"
    gtfs_path = "gtfs_brt.zip"
    bd.download(
        savepath=gps_path,
        query=query,
        billing_project_id="rj-smtr-dev",
        from_file=True,
        index=False,
    )
    bd.Storage("br_rj_riodejaneiro_gtfs_planned", "gtfs_planned").download(
        savepath=gtfs_path, filename="gtfs_version_date=20210419/gtfs_planned3.zip"
    )

    return pd.read_csv(gps_path)


def drop_overlap(df1, df2):
    df2 = df2.astype("str")

    _df = df1.merge(df2, on=df2.columns.to_list(), how="right", indicator=True)

    _df = _df[_df._merge == "right_only"]

    _df.drop("_merge", axis=1, inplace=True)

    return _df


@solid()
def update_realized_trips(context, gps_data):

    rt_filename = f'realized_trips_{date.today().strftime("%Y-%m-%d")}.csv'
    unplanned_filename = f'unplanned_{date.today().strftime("%Y-%m-%d")}.csv'
    rgtfs_path = f'rgtfs_{date.today().strftime("%Y-%m-%d")}'
    gps_path = "brt_daily.csv"
    gtfs_path = "gtfs_brt.zip"

    RT, unplanned = simple.main(
        gtfs_path,
        gps_path,
        rgtfs_path,
        stop_buffer_radius=100,
    )

    unplanned.to_csv(unplanned_filename, index=False)

    realized = Table(
        dataset_id="br_rj_riodejaneiro_gtfs_brt", table_id="realized_trips_gps"
    )

    try:
        ref = realized._get_table_obj("staging")
    except google.api_core.exceptions.NotFound:
        ref = None

    if ref:
        tb = bd.read_table(
            "br_rj_riodejaneiro_gtfs_brt_staging",
            "realized_trips_gps",
            query_project_id="rj-smtr",
            billing_project_id="rj-smtr",
            from_file=True,
        )

        df = drop_overlap(tb, RT)

        df.to_csv(rt_filename, index=False)

        realized.append(filepath=rt_filename, if_exists="pass")
    else:
        RT.to_csv(rt_filename, index=False)

        realized.create(path=rt_filename, if_table_config_exists="replace")

    with open(
        "bases/br_rj_riodejaneiro_brt_gtfs_gps/realized_trips/publish.sql", "aw"
    ) as q:
        query = """
        CREATE VIEW rj-smtr.br_rj_riodejaneiro_brt_gtfs_gps.realized_trips AS
        SELECT 
        SAFE_CAST(vehicle_id AS STRING) vehicle_id,
        SAFE_CAST(route_id AS STRING) route_id,
        SAFE_CAST(direction_id AS INT64) direction_id,
        SAFE_CAST(service_id AS STRING) service_id,
        SAFE_CAST(trip_id AS STRING) trip_id,
        SAFE_CAST(departure_datetime AS DATETIME) departure_datetime,
        SAFE_CAST(arrival_datetime AS DATETIME) arrival_datetime,
        from rj-smtr-staging.br_rj_riodejaneiro_brt_gtfs_gps_staging.realized_trips as t
        """
        q.write(query)
    realized.publish(if_exists="replace")


@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
        )
    ]
)
# tags={"dagster/priority": "10"}
def br_rj_riodejaneiro_brt_gtfs_gps():
    update_realized_trips(get_daily_brt_gps_data())
