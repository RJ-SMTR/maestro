from dagster import schedule
import yaml
from pathlib import Path

def read_config(yaml_file):
    with open(yaml_file, "r") as load_file:
        config = yaml.load(load_file, Loader=yaml.FullLoader)
        return config

@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_riodejaneiro_brt_gps_registros",
    name="br_rj_riodejaneiro_brt_gps_registros",
    mode="br_rj_riodejaneiro_brt_gps_registros"
)
def br_rj_riodejaneiro_brt_gps_registros(date):

    return read_config(Path(__file__).parent / 'registros.yaml')

