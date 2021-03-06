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
    keepalive_key,
    timezone_config,
    discord_webhook,
)
from repositories.libraries.basedosdados.resources import (
    basedosdados_config,
)
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success, redis_keepalive_on_failure, redis_keepalive_on_succes
from repositories.capturas.solids import (
    create_current_datetime_partition,
    get_file_path_and_partitions,
    get_raw,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
)

from repositories.libraries.basedosdados.solids import (
    upload_to_bigquery,
)


@solid(
    required_resource_keys={"basedosdados_config", "timezone_config"},
)
def pre_treatment_br_rj_riodejaneiro_onibus_gps(context, data, timestamp):

    timezone = context.resources.timezone_config["timezone"]

    data = data.json()
    df = pd.DataFrame(data)
    timestamp_captura = pd.to_datetime(timestamp)
    df["timestamp_captura"] = timestamp_captura
    # Remove timezone and force it to be config timezone
    df["datahora"] = df["datahora"].astype(float).apply(
        lambda ms: pd.to_datetime(pendulum.from_timestamp(
            ms / 1000.0).replace(tzinfo=None).set(tz=timezone).isoformat())
    )

    return df


@discord_message_on_failure
@discord_message_on_success
@redis_keepalive_on_failure
@redis_keepalive_on_succes
@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config,
                                  "timezone_config": timezone_config,
                                  "discord_webhook": discord_webhook,
                                  "keepalive_key": keepalive_key}
        ),
    ],
    # tags={"dagster/priority": "10"}
)
def br_rj_riodejaneiro_onibus_gps_registros():

    filename, partitions = create_current_datetime_partition()
    file_path = get_file_path_and_partitions(filename, partitions)

    data, timestamp, error = get_raw()

    upload_logs_to_bq(timestamp,error)

    raw_file_path = save_raw_local(data, file_path)

    treated_data = pre_treatment_br_rj_riodejaneiro_onibus_gps(data,timestamp)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery([raw_file_path, treated_file_path], partitions)
