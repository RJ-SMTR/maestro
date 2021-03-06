from dagster import schedule
import yaml
from pathlib import Path
from repositories.helpers.helpers import read_config


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_riodejaneiro_brt_gps_registros",
    name="br_rj_riodejaneiro_brt_gps_registros",
    mode="dev",
    execution_timezone="America/Sao_Paulo"
)
def br_rj_riodejaneiro_brt_gps_registros(context):
    timezone = context.scheduled_execution_time.timezone.name
    config = read_config(Path(__file__).parent / 'br_rj_riodejaneiro_brt_gps/registros.yaml') 
    config["resources"]["timezone_config"]["config"]["timezone"] = timezone
    return config


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_riodejaneiro_onibus_gps_registros",
    name="br_rj_riodejaneiro_onibus_gps_registros",
    mode="dev",
    execution_timezone="America/Sao_Paulo"
)
def br_rj_riodejaneiro_onibus_gps_registros(context):
    timezone = context.scheduled_execution_time.timezone.name
    config = read_config(Path(__file__).parent / 'br_rj_riodejaneiro_onibus_gps/registros.yaml') 
    config["resources"]["timezone_config"]["config"]["timezone"] = timezone
    return config