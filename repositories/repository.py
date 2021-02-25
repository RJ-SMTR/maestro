from dagster import repository
import sys

import repositories.capturas.br_rj_riodejaneiro_brt_gps.schedulers
import repositories.capturas.br_rj_riodejaneiro_brt_gps.registros


@repository
def capturas():
    return [
        repositories.capturas.br_rj_riodejaneiro_brt_gps.schedulers.br_rj_riodejaneiro_brt_gps_registros,
        repositories.capturas.br_rj_riodejaneiro_brt_gps.registros.br_rj_riodejaneiro_brt_gps_registros,
    ]
