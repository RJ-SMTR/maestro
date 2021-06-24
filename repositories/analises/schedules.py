from dagster import daily_schedule
from pathlib import Path
from datetime import datetime, time
from repositories.helpers.helpers import read_config


@daily_schedule(
    pipeline_name="br_rj_riodejaneiro_brt_gtfs_gps",
    start_date=datetime(2021, 1, 1),
    name="br_rj_riodejaneiro_brt_gtfs_gps",
    execution_time=time(0, 30),
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def br_rj_riodejaneiro_brt_gtfs_gps(date):
    return {
        "solids": {
            "get_daily_brt_data": {"config": {"date": datetime}},
            "update_realized_trips": {"config": {"date": datetime}},
        }
    }
