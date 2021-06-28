from dagster import daily_schedule
from pathlib import Path
from datetime import datetime, time
from repositories.helpers.helpers import read_config


@daily_schedule(
    pipeline_name="br_rj_riodejaneiro_brt_gtfs_gps_realized_trips",
    start_date=datetime(2021, 1, 1),
    name="br_rj_riodejaneiro_brt_gtfs_gps",
    execution_time=time(17, 56),
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def br_rj_riodejaneiro_brt_gtfs_gps_realized_trips(date):
    config = read_config(
        Path(__file__).parent / "br_rj_riodejaneiro_brt_gtfs_gps/realized_trips.yaml"
    )
    solids = [
        "get_daily_brt_gps_data",
        "download_gtfs_from_storage",
        "update_realized_trips",
    ]
    solid_config = {
        "solids": {
            solid: {"config": {"date": date.strftime("%Y-%m-%d")}} for solid in solids
        }
    }
    config.update(solid_config)

    return config
