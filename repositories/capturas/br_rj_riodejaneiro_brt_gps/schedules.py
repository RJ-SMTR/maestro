from dagster import schedule
import yaml
from pathlib import Path
from repositories.helpers.helpers import read_config


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_riodejaneiro_brt_gps_registros",
    name="br_rj_riodejaneiro_brt_gps_registros",
    mode="dev"
)
def br_rj_riodejaneiro_brt_gps_registros(date):

    return read_config(Path(__file__).parent / 'registros.yaml')

