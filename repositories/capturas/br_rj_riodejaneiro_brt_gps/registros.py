from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    PresetDefinition,
)

import pendulum
import pandas as pd
from pathlib import Path

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


@solid(
    required_resource_keys={"basedosdados_config", "timezone_config"},
)
def pre_treatment_br_rj_riodejaneiro_brt_gps(context, data):

    timezone = context.resources.timezone_config["timezone"]

    data = data.json()
    df = pd.DataFrame(data["veiculos"])
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    df["dataHora"] = df["dataHora"].apply(
        lambda ms: pd.to_datetime(pendulum.from_timestamp(ms / 1000.0, timezone).isoformat())
    )

    return df


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config, 
                                  "timezone_config": timezone_config,
                                  "discord_webhook": discord_webhook}
        ),
    ]
)
def br_rj_riodejaneiro_brt_gps_registros():

    file_path, partitions = get_file_path_and_partitions()

    data = get_raw()

    raw_file_path = save_raw_local(data, file_path)

    treated_data = pre_treatment_br_rj_riodejaneiro_brt_gps(data)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery(treated_file_path, raw_file_path, partitions)


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
def br_rj_riodejaneiro_brt_gps_registros_init():

    file_path, partitions = get_file_path_and_partitions()

    data = get_raw()

    raw_file_path = save_raw_local(data, file_path)

    treated_data = pre_treatment_br_rj_riodejaneiro_brt_gps(data)

    treated_file_path = save_treated_local(treated_data, file_path)

    create_table_bq(treated_file_path)