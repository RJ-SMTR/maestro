from dagster import repository
import sys

from repositories.capturas.br_rj_gps_brt.pipeline import hello_world_pipeline


@repository
def hello_world_repository():
    return [hello_world_pipeline]