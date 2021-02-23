from dagster import pipeline, schedule

from repositories.capturas.br_rj_gps_brt.solids import hello_world_pipeline


@schedule(
    cron_schedule="* * * * *",
    pipeline_name="hello_world_pipeline",
    name="hello_world_scheduler",
)
def hello_world_scheduler(date):

    return {}
