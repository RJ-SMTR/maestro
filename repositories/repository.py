from dagster import repository
import sys

import repositories.capturas.br_rj_gps_brt.schedulers
import repositories.capturas.br_rj_gps_brt.registros


@repository
def capturas():
    return [
        repositories.capturas.br_rj_gps_brt.schedulers.br_rj_gps_brt_registros,
        repositories.capturas.br_rj_gps_brt.registros.br_rj_gps_brt_registros,
    ]
