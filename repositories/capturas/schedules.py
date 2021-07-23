from dagster import schedule, daily_schedule
from pathlib import Path
from repositories.helpers.helpers import read_config
from datetime import datetime, time


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_riodejaneiro_brt_gps_registros",
    name="br_rj_riodejaneiro_brt_gps_registros",
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def br_rj_riodejaneiro_brt_gps_registros(context):
    timezone = context.scheduled_execution_time.timezone.name
    config = read_config(
        Path(__file__).parent / "br_rj_riodejaneiro_brt_gps/registros.yaml"
    )
    config["resources"]["timezone_config"]["config"]["timezone"] = timezone
    return config


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_riodejaneiro_onibus_gps_registros",
    name="br_rj_riodejaneiro_onibus_gps_registros",
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def br_rj_riodejaneiro_onibus_gps_registros(context):
    timezone = context.scheduled_execution_time.timezone.name
    config = read_config(
        Path(__file__).parent / "br_rj_riodejaneiro_onibus_gps/registros.yaml"
    )
    config["resources"]["timezone_config"]["config"]["timezone"] = timezone
    return config


@daily_schedule(
    pipeline_name="br_rj_riodejaneiro_sigmob_data",
    start_date=datetime(2021, 1, 1),
    name="br_rj_riodejaneiro_sigmob_data",
    execution_time=time(5, 0),
    mode="dev",
    execution_timezone="America/Sao_Paulo",
)
def br_rj_riodejaneiro_sigmob_data(date):
    config = read_config(Path(__file__).parent / "br_rj_riodejaneiro_sigmob/data.yaml")
    config["resources"]["schedule_run_date"] = {
        "config": {"date": date.strftime("%Y-%m-%d")}
    }

    return config
