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


@daily_schedule(
    pipeline_name="projeto_multa_automatica_sumario_multa_onibus_integrado_stu",
    start_date=datetime(2021, 1, 1),
    name="projeto_multa_automatica_sumario_multa_onibus_integrado_stu",
    execution_time=time(21, 30),
    mode="dev",
    execution_timezone="America/Sao_Paulo",
    partition_days_offset=0,
)
def projeto_multa_automatica_sumario_multa_onibus_integrado_stu(date):
    config = read_config(
        Path(__file__).parent
        / "projeto_multa_automatica/sumario_multa_onibus_integrado_stu.yaml"
    )
    config["resources"]["schedule_run_date"] = {
        "config": {"date": date.strftime("%Y-%m-%d")}
    }

    return config
