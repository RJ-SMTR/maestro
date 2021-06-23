from dagster import schedule
from pathlib import Path
from repositories.helpers.helpers import read_config


@schedule(
    cron_schedule="30 0 * * *",
    pipeline_name="br_rj_riodejaneiro_brt_gtfs_gps",
    name="br_rj_riodejaneiro_brt_gtfs_gps",
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def br_rj_riodejaneiro_brt_gtfs_gps(context):
    timezone = context.scheduled_execution_time.timezone.name
    config = read_config(
        Path(__file__).parent
        / "br_rj_riodejaneiro_brt_gtfs_gps/realized_trips_gps.yaml"
    )
    config["resources"]["timezone_config"]["config"]["timezone"] = timezone
    return config
