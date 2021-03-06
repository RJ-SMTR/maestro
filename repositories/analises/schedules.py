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
def br_rj_riodejaneiro_brt_gtfs_gps_realized_trips(date):
    config = read_config(
        Path(__file__).parent / "br_rj_riodejaneiro_brt_gtfs_gps/realized_trips.yaml"
    )
    config["resources"]["schedule_run_date"] = {
        "config": {"date": date.strftime("%Y-%m-%d")}
    }

    return config
