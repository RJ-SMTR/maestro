from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    PresetDefinition,
    OutputDefinition,
    Output,
    resource,
    Field,
    composite_solid,
)

from pathlib import Path
import os
import re
import pandas as pd
import pendulum
from openpyxl import load_workbook

import basedosdados as bd
from google.cloud import storage

from repositories.capturas.resources import (
    basedosdados_config,
    timezone_config,
    discord_webhook,
)
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success
from repositories.capturas.solids import (
    get_file_path_and_partitions, 
    get_raw,
    save_raw_local,
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
"GRATUIDADE ESTUDANTE FEDERAL": "gratuidade_estudande_federal",
"GRATUIDADE ESTUDANTE ESTADUAL": "gratuidade_estudande_estadual",
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

@resource(
    {
        "bucket_name": Field(
            str, is_required=True, description="Bucket name from Google Cloud Storage"
        ),
        "bucket_path": Field(
            str, is_required=True, description="Path for file stored in GCS"
        ),
    }
)
def storage_config(context):
    return context.resource_config


@solid(
    output_defs=[
        OutputDefinition(name="file_path"),
        OutputDefinition(name="filetype"),
        OutputDefinition(name="partitions"),
    ],
    required_resource_keys={"storage_config"},
)
def parse_file_path_and_partitions(context):

    bucket_path = context.resources.storage_config["bucket_path"]

    # Parse bucket to get mode, dataset_id, table_id and filename
    path_list = bucket_path.split('/')
    mode = path_list[0]
    dataset_id = path_list[1]
    table_id = path_list[2]
    filename = path_list[-1].split(".")[0]
    filetype = path_list[-1].split(".")[1]

    # Parse bucket to get partitions
    partitions = re.findall("\/([^\/]*?)=(.*?)(?=\/)", bucket_path)
    partitions = "/".join(["=".join([field for field in item]) for item in partitions])
        
    file_path = f"{os.getcwd()}/data/{{mode}}/{dataset_id}/{table_id}/{partitions}/{filename}.{{filetype}}"
    context.log.info(f"creating file path {file_path}")

    yield Output(file_path, output_name="file_path")
    yield Output(filetype, output_name="filetype")
    yield Output(partitions, output_name="partitions")


@solid(required_resource_keys={"storage_config"})
def get_file_from_storage(context, file_path, mode='raw', filetype="xlsx"):

    # Download from storage
    bucket_name = context.resources.storage_config["bucket_name"]
    bucket_path = context.resources.storage_config["bucket_path"]

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(bucket_path)
    _file_path = file_path.format(mode=mode, filetype=filetype)
    blob.download_to_filename(_file_path)

    return _file_path

@solid 
def get_header(context):
    return ORIGINAL_HEADER

@solid
def delete_xls_header(context, file_path):
    wb = load_workbook(file_path)
    ws =  wb.active
    # Delete current header
    ws.delete_rows(1)
    wb.save(file_path)
    return file_path


@solid
def set_xls_header(context, file_path, header):
    wb = load_workbook(file_path)
    ws =  wb.active
    ws.insert_rows(1)
    for idx, column in enumerate(header):
        ws.cell(row=1, column=idx+1).value = column
    wb.save(file_path)

    return file_path

@composite_solid
def set_header(file_path, header):
    file_path = delete_xls_header(file_path)
    file_path = set_xls_header(file_path, header)
    return file_path


@solid(
    required_resource_keys={"basedosdados_config", "timezone_config"},
)
def pre_treatment_br_rj_riodejaneiro_brt_rdo(context, file_path):

    timezone = context.resources.timezone_config["timezone"]

    # Read from Excel
    df = pd.read_excel(file_path, engine='openpyxl')
    df.rename(columns = column_mapping, inplace = True)
    df = df.reindex(columns = ORDERED_HEADER)
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    context.log.debug(", ".join(list(df.columns)))

    return df



@discord_message_on_failure
@discord_message_on_success
@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config, 
                                  "timezone_config": timezone_config,
                                  "discord_webhook": discord_webhook,
                                  "storage_config": storage_config}
        ),
    ]
)
def br_rj_riodejaneiro_brt_rdo_registros():

    file_path, filetype, partitions = parse_file_path_and_partitions()

    raw_file_path = get_file_from_storage(file_path=file_path, filetype=filetype)

    header = get_header()

    raw_header_file_path = set_header(raw_file_path, header)

    treated_data = pre_treatment_br_rj_riodejaneiro_brt_rdo(raw_header_file_path)

    treated_file_path = save_treated_local(treated_data, file_path)

    # # TODO: REFAZER A FUNÇÃO PARA SUBIR SÓ STAGING
    # upload_to_bigquery(treated_file_path, raw_file_path, partitions)
    create_table_bq(treated_file_path)