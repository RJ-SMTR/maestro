from dagster import schedule


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_riodejaneiro_brt_gps_registros",
    name="br_rj_riodejaneiro_brt_gps_registros",
)
def br_rj_riodejaneiro_brt_gps_registros(date):

    return {}
