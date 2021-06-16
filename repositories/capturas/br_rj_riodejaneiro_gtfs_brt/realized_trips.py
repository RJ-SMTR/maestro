import requests
import json
import time
from pathlib import Path
import pandas as pd
from datetime import timedelta, datetime
import yaml
from rgtfs.helpers import treat_rj_brt_realized_trips

from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    PresetDefinition,
    OutputDefinition,
    Output,
    Partition,
    PartitionSetDefinition,
)

from repositories.capturas.resources import (
    keepalive_key,
    timezone_config,
    discord_webhook,
)
from repositories.libraries.basedosdados.resources import (
    basedosdados_config,
)
from repositories.helpers.hooks import (
    discord_message_on_failure,
    discord_message_on_success,
    redis_keepalive_on_failure,
    redis_keepalive_on_succes,
)
from repositories.capturas.solids import (
    save_local_as_bd,
)


from repositories.libraries.basedosdados.solids import (
    upload_to_bigquery_v2,
)


@solid(
    required_resource_keys={"basedosdados_config"},
    output_defs=[
        OutputDefinition(name="raw_file_path"),
        OutputDefinition(name="file_name"),
    ],
    config_schema={"date": str},
)
def download_brt_raw_realized_trips(context, post_url, data_url):

    date = context.solid_config["date"]

    date = pd.Timestamp(date)

    payload = {
        "formato": "2",
        "horaInicial": "00:00:00",
        "horaFinal": "23:59:59",
        "dataInicial": date.strftime("%Y-%m-%d 00:00:00"),
        "dataFinal": date.strftime("%Y-%m-%d 23:59:59"),
        "linhasSelecionadas": [],
        "empresasSelecionadas": [],
        "trajetosSelecionados": [],
        "veiculosSelecionados": [],
        "motoristasSelecionados": [],
        "statusSelecionados": [],
        "tipoRelatorio": "0",
        "id_usuario": "18118",
        "gmtCliente": "America/Fortaleza",
        "Empresa": [
            {"id_empresa": 864},
            {"id_empresa": 1842},
            {"id_empresa": 861},
            {"id_empresa": 2427},
            {"id_empresa": 1838},
            {"id_empresa": 1843},
            {"id_empresa": 1848},
            {"id_empresa": 2001},
            {"id_empresa": 2428},
            {"id_empresa": 1849},
            {"id_empresa": 1850},
            {"id_empresa": 1851},
            {"id_empresa": 2430},
            {"id_empresa": 2431},
            {"id_empresa": 2432},
        ],
        "restricaoEmpresas": [
            864,
            1842,
            861,
            2427,
            1838,
            1843,
            1848,
            2001,
            2428,
            1849,
            1850,
            1851,
            2430,
            2431,
            2432,
        ],
        "preferencias": [
            {"cabecalho": "Data", "conteudo": "dataInicio"},
            {"cabecalho": "Trajeto", "conteudo": "trajetoDescricao"},
            {"cabecalho": "Veiculo Planejado", "conteudo": "veiculoPlanejado"},
            {"cabecalho": "Veiculo Real", "conteudo": "veiculoPrefixo"},
            {"cabecalho": "Motorista", "conteudo": "matriculaMotorista"},
            {"cabecalho": "Chegada ao ponto", "conteudo": "chegadaPonto"},
            {"cabecalho": "Partida Planejada", "conteudo": "dataInicioPlanejada"},
            {"cabecalho": "Partida Real", "conteudo": "horarioInicio"},
            {"cabecalho": "Diff Partida", "conteudo": "diffPartida"},
            {"cabecalho": "HE", "conteudo": "he"},
            {"cabecalho": "Chegada Planejada", "conteudo": "dataFimPlanejada"},
            {"cabecalho": "Chegada Real", "conteudo": "dataFim"},
            {"cabecalho": "Diff Chegada", "conteudo": "diffChegada"},
            {"cabecalho": "Tempo Viagem", "conteudo": "tempoViagemFormatado"},
            {"cabecalho": "KM Executado", "conteudo": "kmExecutado"},
            {"cabecalho": "Vel. Media Km", "conteudo": "velocidadeMedia"},
            {"cabecalho": "Temp.Ponto", "conteudo": "tempoPonto"},
            {"cabecalho": "Passageiro", "conteudo": "passageiros"},
            {"cabecalho": "I.P.K", "conteudo": "ipkFormatado"},
            {"cabecalho": "Status da Viagem", "conteudo": "statusControlePartida"},
            {"cabecalho": "Viagem Editada", "conteudo": "usuarioCriacao"},
            {"cabecalho": "Tabela", "conteudo": "tabelaId"},
        ],
        "enviarParaWorker": True,
        "nomeCliente": "CONSORCIO OPERACIONAL BRT",
        "idCliente": "209",
        "nomeUsuario": "joao.carabetta",
    }

    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.86 Safari/537.36",
        "content-type": "application/json;charset=UTF-8",
        "accept": "*/*",
        "sec-gpc": "1",
        "origin": "http://zn4.m2mcontrol.com.br",
        "referer": "http://zn4.m2mcontrol.com.br/",
        "accept-encoding": "gzip, deflate",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "postman-token": "d7202971-3172-4efa-6679-6154f6886655",
    }

    response = requests.request(
        "POST",
        post_url,
        data=json.dumps(payload),
        headers=headers,
    ).json()

    time.sleep(5)

    data_url = f'{data_url}_{response["id"]}.csv'
    data = requests.get(data_url).text

    file_name = date.strftime("%Y-%m-%d.csv")
    raw_file_path = save_local_as_bd(
        data,
        data_folder="data",
        file_name=file_name,
        dataset_id=context.resources.basedosdados_config["dataset_id"],
        table_id=context.resources.basedosdados_config["table_id"],
        mode="raw",
        filetype="csv",
    )

    yield Output(raw_file_path, output_name="raw_file_path")
    yield Output(file_name, output_name="file_name")


@solid(
    required_resource_keys={"basedosdados_config"},
)
def treat_raw_realized_trips(context, raw_file_path, file_name):

    realized_trips = treat_rj_brt_realized_trips(raw_file_path)

    treated_file_path = save_local_as_bd(
        realized_trips,
        data_folder="data",
        file_name=file_name,
        dataset_id=context.resources.basedosdados_config["dataset_id"],
        table_id=context.resources.basedosdados_config["table_id"],
        mode="staging",
        filetype="csv",
    )

    return treated_file_path


@discord_message_on_failure
@discord_message_on_success
@redis_keepalive_on_failure
@redis_keepalive_on_succes
@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev",
            resource_defs={
                "basedosdados_config": basedosdados_config,
                "timezone_config": timezone_config,
                "discord_webhook": discord_webhook,
                "keepalive_key": keepalive_key,
            },
        ),
    ],
    preset_defs=[
        PresetDefinition.from_files(
            "realized_trips",
            config_files=[str(Path(__file__).parent / "realized_trips.yaml")],
            mode="dev",
        ),
    ],
    # tags={"dagster/priority": "-1"}
)
def br_rj_riodejaneiro_gtfs_realized_trips():

    raw_file_path, file_name = download_brt_raw_realized_trips()
    treated_file_path = treat_raw_realized_trips(raw_file_path, file_name)

    upload_to_bigquery_v2.alias(
        "upload_raw_realized_trips")(file_path=raw_file_path)
    upload_to_bigquery_v2.alias("upload_realized_trips")(
        file_path=treated_file_path)


def get_date_partitions():
    """Every day in the month of May, 2020"""
    start_date = datetime(2021, 1, 1)
    end_date = datetime.now()

    partitions = []
    while start_date < end_date:

        partitions.append(Partition(start_date.strftime("%Y-%m-%d")))
        start_date = start_date + timedelta(days=1)

    return partitions


def run_config_for_date_partition(partition):
    date = partition.value
    config = yaml.load(
        open(Path(__file__).parent / "realized_trips.yaml", "r"))
    config["solids"]["download_brt_raw_realized_trips"]["config"]["date"] = date
    return config


daily_partition_set = PartitionSetDefinition(
    name="daily_partitions",
    pipeline_name="br_rj_riodejaneiro_gtfs_realized_trips",
    partition_fn=get_date_partitions,
    run_config_fn_for_partition=run_config_for_date_partition,
    mode="dev",
)


def daily_partition_selector(context, partition_set):

    partitions = partition_set.get_partitions(context.scheduled_execution_time)
    return partitions[-2]  # one day before run day (today)


daily_schedule = daily_partition_set.create_schedule_definition(
    "br_rj_riodejaneiro_gtfs_realized_trips",
    "0 1 * * *",
    partition_selector=daily_partition_selector,
    execution_timezone="America/Sao_Paulo",
)
