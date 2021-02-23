from dagster import schedule


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="br_rj_gps_brt_registros",
    name="br_rj_gps_brt_registros",
)
def br_rj_gps_brt_registros(date):

    return {}
