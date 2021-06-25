from dagster import daily_schedule
from pathlib import Path
from datetime import datetime, time
from repositories.helpers.helpers import read_config


@daily_schedule(
    pipeline_name="br_rj_riodejaneiro_brt_gtfs_gps_realized_trips",
    start_date=datetime(2021, 1, 1),
    name="br_rj_riodejaneiro_brt_gtfs_gps",
    execution_time=time(0, 30),
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def br_rj_riodejaneiro_brt_gtfs_gps(date):
    return {
        "solids": {
            "get_daily_brt_gps_data": {"config": {"date": date.strftime("%Y-%m-%d")}},
            "download_gtfs_from_storage": {
                "config": {"date": date.strftime("%Y-%m-%d")}
            },
            "update_realized_trips": {"config": {"date": date.strftime("%Y-%m-%d")}},
        }
    }
