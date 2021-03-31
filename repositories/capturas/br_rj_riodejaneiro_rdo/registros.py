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
"TERMO",
"LINHA",
"SERVIÇO",
"TERMO SERVIÇO",
"TIPO DE VEICULO",
"DATA ANO",
"DATA MÊS",
"DATA DIA",
"TARIFA CÓDIGO",
"TARIFA VALOR",
"FROTA DETERMINADA",
"FROTA LICENCIADA",
"FROTA OPERANTE",
"VIAGEM REALIZADA",
"KM",
"GRATUIDADE IDOSO",
"GRATUIDADE ESPECIAL",
"GRATUIDADE ESTUDANTE FEDERAL",
"GRATUIDADE ESTUDANTE ESTADUAL",
"GRATUIDADE ESTUDANTE MUNICIPAL",
"GRATUIDADE RODOVIARIO",
"GRATUIDADE TOTAL",
"BUC 1ª PERNA",
"BUC 2ª PERNA",
"BUC RECEITA",
"BUC SUPERVIA 1ª PERNA",
"BUC SUPERVIA 2ª PERNA",
"BUC SUPERVIA RECEITA",
"VT PASSAGEIRO TRANSPORTADO",
"VT RECEITA",
"ESPECIE PASSAGEIRO TRANSPORTADO",
"ESPECIE RECEITA",
"TOTAL PASSAGEIRO TRANSPORTADO",
"TOTAL RECEITA",
"TIPO DE INFORMAÇÃO",
"UNIVERSITARIO",
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
"gratuidade_estudante_federal",
"gratuidade_estudante_estadual",
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


COLUMN_MAPPING = {
"TERMO": "operadora",
"LINHA": "linha",
"SERVIÇO": "servico_tipo",
"TERMO SERVIÇO": "servico_termo",
"TIPO DE VEICULO": "tipo_veiculo",
"DATA ANO": "data_ano",
"DATA MÊS": "data_mes",
"DATA DIA": "data_dia",
"TARIFA CÓDIGO": "tarifa_codigo",
"TARIFA VALOR": "tarifa_valor",
"FROTA DETERMINADA": "frota_determinada",
"FROTA LICENCIADA": "frota_licenciada",
"FROTA OPERANTE": "frota_operante",
"VIAGEM REALIZADA": "viagem_realizada",
"KM": "km",
"GRATUIDADE IDOSO": "gratuidade_idoso",
"GRATUIDADE ESPECIAL": "gratuidade_especial",
"GRATUIDADE ESTUDANTE FEDERAL": "gratuidade_estudante_federal",
"GRATUIDADE ESTUDANTE ESTADUAL": "gratuidade_estudante_estadual",
"GRATUIDADE ESTUDANTE MUNICIPAL": "gratuidade_estudante_municipal",
"GRATUIDADE RODOVIARIO": "gratuidade_rodoviario",
"UNIVERSITARIO": "universitario",
"GRATUIDADE TOTAL": "gratuidade_total",
"BUC 1ª PERNA": "buc_1a_perna",
"BUC 2ª PERNA": "buc_2a_perna",
"BUC RECEITA": "buc_receita",
"BUC SUPERVIA 1ª PERNA": "buc_supervia_1a_perna",
"BUC SUPERVIA 2ª PERNA": "buc_supervia_2a_perna",
"BUC SUPERVIA RECEITA": "buc_supervia_receita",
"VT PASSAGEIRO TRANSPORTADO": "perna_unica_e_outros_transportado",
"VT RECEITA": "perna_unica_e_outros_receita",
"ESPECIE PASSAGEIRO TRANSPORTADO": "especie_passageiro_transportado",
"ESPECIE RECEITA": "especie_receita",
"TOTAL PASSAGEIRO TRANSPORTADO": "total_passageiro_transportado",
"TOTAL RECEITA": "total_receita",
"TIPO DE INFORMAÇÃO": "tipo_informacao",
}

@solid(
    output_defs=[
        OutputDefinition(name="original_header"),
        OutputDefinition(name="column_mapping"),
        OutputDefinition(name="ordered_header"),
    ],
)
def get_headers(context, original_header, column_mapping, ordered_header):
    yield Output(original_header, output_name="original_header")
    yield Output(column_mapping, output_name="column_mapping")
    yield Output(ordered_header, output_name="ordered_header")


@solid
def process_csv(context, file_path, original_header, column_mapping, ordered_header):

    # Rearrange columns
    df = pd.read_csv(file_path, delimiter=";")
    df.columns = original_header
    for item in column_mapping:
        context.log.debug("%s" % item)
        context.log.debug("%s" % type(item))
    context.log.debug("%s" % type(COLUMN_MAPPING))
    df.rename(columns = column_mapping, inplace = True)
    df = df.reindex(columns = ordered_header)
    return df

@solid(
    required_resource_keys={"timezone_config"},
)
def add_timestamp(context ,df):
    timezone = context.resources.timezone_config["timezone"]
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    context.log.debug(", ".join(list(df.columns)))

    return df

@solid
def divide_columns(context, df, cols_to_divide, value=100):
    # Divide columns by value
    df[cols_to_divide] = df[cols_to_divide].apply(lambda x: x/value, axis=1)
    return df

# @solid(
#     required_resource_keys={"timezone_config"},
# )
# def transform(context, treated_data, **args):
#     transformations = context.solid_config["transformations"]
#     for item in transformations:
#         treated_data = eval(item)(treated_data, **args)


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    preset_defs=[
        PresetDefinition.from_files(
            "BRT_RDO_40",
            config_files=[str(Path(__file__).parent / "registros_rdo40_brt.yaml")],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "Onibus_RDO_40",
            config_files=[str(Path(__file__).parent / "registros_rdo40_onibus.yaml")],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "RDO5",
            config_files=[str(Path(__file__).parent / "registros_rdo5.yaml")],
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
def br_rj_riodejaneiro_rdo_registros():

    filename, filetype, file_path, partitions = parse_file_path_and_partitions()

    raw_file_path = get_file_from_storage(file_path=file_path, filename=filename, 
                                          partitions=partitions, filetype=filetype)

    # Extract, load and transform
    original_header, column_mapping, ordered_header = get_headers()
    treated_data = process_csv(raw_file_path, original_header, column_mapping, ordered_header)
    treated_data = add_timestamp(treated_data)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery([treated_file_path], partitions)


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    preset_defs=[
        PresetDefinition.from_files(
            "BRT_RDO_40",
            config_files=[str(Path(__file__).parent / "registros_rdo40_brt.yaml")],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "Onibus_RDO_40",
            config_files=[str(Path(__file__).parent / "registros_rdo40_onibus.yaml")],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "RDO5",
            config_files=[str(Path(__file__).parent / "registros_rdo5.yaml")],
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
def br_rj_riodejaneiro_rdo_registros_init():
    filename, filetype, file_path, partitions = parse_file_path_and_partitions()

    raw_file_path = get_file_from_storage(file_path=file_path, filename=filename, 
                                          partitions=partitions, filetype=filetype)

    # Extract, load and transform
    original_header, column_mapping, ordered_header = get_headers()
    treated_data = process_csv(raw_file_path, original_header, column_mapping, ordered_header)
    treated_data = add_timestamp(treated_data)

    treated_file_path = save_treated_local(treated_data, file_path)

    create_table_bq(treated_file_path)