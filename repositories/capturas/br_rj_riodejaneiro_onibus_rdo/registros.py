from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    OutputDefinition,
    Output,
    composite_solid,
    PresetDefinition,
)

from pathlib import Path
import os
import re
import pandas as pd
import pendulum
from openpyxl import load_workbook

import basedosdados as bd

from repositories.capturas.resources import (
    basedosdados_config,
    timezone_config,
    discord_webhook,
)
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success
from repositories.capturas.solids import (
    get_file_from_storage,
    parse_file_path_and_partitions, 
    save_treated_local,
    upload_to_bigquery,
    create_table_bq,
)

ORIGINAL_HEADER = [
"Termo",
"Linha",
"TPSERV",
"ORD_SERV",
"TPVEIC",
"ANO",
"MÊS",
"DIA",
"TARIDET",
"TARIFPRA",
"FROTDET",
"FROTLIC",
"FROTOPE",
"NUMVIG",
"KMCOB",
"IDOSO",
"PNE",
"ESTU",
"ESTE",
"ESTM",
"FUNCEMP",
"TOTGRAT",
"BUC",
"BUCINT",
"RECBUC",
"BUCS",
"BUCSINT",
"RECBUCS",
"BUTMET",
"RECVT",
"TOTPASPAG",
"RECPASPAG",
"TOTPAS",
"RECTOTAL",
"PASLVRUNI",
]

ORDERED_HEADER = [
"operadora",
"linha",
"servico_tipo",
"servico_termo",
"tipo_veiculo",
"data_ano",
"data_mes",
"data_dia",
"tarifa_codigo",
"tarifa_valor",
"frota_determinada",
"frota_licenciada",
"frota_operante",
"viagem_realizada",
"km",
"gratuidade_idoso",
"gratuidade_especial",
"gratuidade_estudande_federal",
"gratuidade_estudande_estadual",
"gratuidade_estudante_municipal",
"gratuidade_rodoviario",
"universitario",
"gratuidade_total",
"buc_1a_perna",
"buc_2a_perna",
"buc_receita",
"buc_supervia_1a_perna",
"buc_supervia_2a_perna",
"buc_supervia_receita",
"perna_unica_e_outros_transportado",
"perna_unica_e_outros_receita",
"especie_passageiro_transportado",
"especie_receita",
"total_passageiro_transportado",
"total_receita",
"tipo_informacao",
]


column_mapping = {
"Termo": "operadora",
"Linha": "linha",
"TPSERV": "serviço_tipo",
"ORD_SERV": "serviço_termo",
"TPVEIC": "tipo_veículo",
"ANO": "data_ano",
"MÊS": "data_mês",
"DIA": "data_dia",
"TARIDET": "tarifa_código",
"TARIFPRA": "tarifa_valor",
"FROTDET": "frota_determinada",
"FROTLIC": "frota_licenciada",
"FROTOPE": "frota_operante",
"NUMVIG": "viagem_realizada",
"KMCOB": "km",
"IDOSO": "gratuidade_idoso",
"PNE": "gratuidade_especial",
"ESTU": "gratuidade_estudande_federal",
"ESTE": "gratuidade_estudande_estadual",
"ESTM": "gratuidade_estudante_municipal",
"FUNCEMP": "gratuidade_rodoviário",
"PASLVRUNI": "universitário",
"TOTGRAT": "gratuidade_total",
"BUC": "buc_1ª_perna",
"BUCINT": "buc_2ª_perna",
"RECBUC": "buc_receita",
"BUCS": "buc_supervia_1ª_perna",
"BUCSINT": "buc_supervia_2ª_perna",
"RECBUCS": "buc_supervia_receita",
"BUTMET": "perna_unica_e_outros_transportado",
"RECVT": "perna_unica_e_outros_receita",
"TOTPASPAG": "espécie_passageiro_transportado",
"RECPASPAG": "espécie_receita",
"TOTPAS": "total_passageiro_transportado",
"RECTOTAL": "total_receita",
}

cols_to_divide = [
    "tarifa_valor",
    "buc_receita",
    "buc_supervia_receita",
    "perna_unica_e_outros_receita",
    "especie_receita",
    "total_receita"
]


@solid(
    required_resource_keys={"basedosdados_config", "timezone_config"},
)
def pre_treatment_br_rj_riodejaneiro_onibus_rdo(context, file_path):

    timezone = context.resources.timezone_config["timezone"]

    # Rearrange columns
    df = pd.read_excel(file_path, engine='openpyxl')
    df.columns = ORIGINAL_HEADER
    df.rename(columns = column_mapping, inplace = True)
    df = df.reindex(columns = ORDERED_HEADER)
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    context.log.debug(", ".join(list(df.columns)))

    # Divide money columns by 100
    df[cols_to_divide] = df[cols_to_divide].apply(lambda x: x/100, axis=1)

    return df



@discord_message_on_failure
@discord_message_on_success
@pipeline(
    preset_defs=[
        PresetDefinition.from_files(
            "init",
            config_files=[str(Path(__file__).parent / "registros.yaml")],
            mode="dev",
        )
    ],
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config, 
                                  "timezone_config": timezone_config,
                                  "discord_webhook": discord_webhook}
        ),
    ],
)
def br_rj_riodejaneiro_onibus_rdo_registros():

    filename, filetype, file_path, partitions = parse_file_path_and_partitions()

    raw_file_path = get_file_from_storage(file_path=file_path, filename=filename, 
                                          partitions=partitions, filetype=filetype)

    treated_data = pre_treatment_br_rj_riodejaneiro_onibus_rdo(raw_file_path)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery([treated_file_path], partitions)


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    preset_defs=[
        PresetDefinition.from_files(
            "init",
            config_files=[str(Path(__file__).parent / "registros.yaml")],
            mode="dev",
        )
    ],
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config, 
                                  "timezone_config": timezone_config,
                                  "discord_webhook": discord_webhook}
        ),
    ],
)
def br_rj_riodejaneiro_onibus_rdo_registros_init():

    filename, filetype, file_path, partitions = parse_file_path_and_partitions()

    raw_file_path = get_file_from_storage(file_path=file_path, filename=filename, 
                                          partitions=partitions, filetype=filetype)

    treated_data = pre_treatment_br_rj_riodejaneiro_onibus_rdo(raw_file_path)

    treated_file_path = save_treated_local(treated_data, file_path)

    create_table_bq(treated_file_path)