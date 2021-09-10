import traceback
from pathlib import Path
from datetime import timedelta

import pendulum
import pandas as pd
from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    PresetDefinition,
)

from repositories.capturas.resources import (
    keepalive_key,
    timezone_config,
    discord_webhook,
)
from repositories.libraries.basedosdados.resources import basedosdados_config
from repositories.helpers.hooks import (
    discord_message_on_failure,
    discord_message_on_success,
    redis_keepalive_on_failure,
    redis_keepalive_on_succes,
    log_critical,
)
from repositories.capturas.solids import (
    create_current_datetime_partition,
    get_file_path_and_partitions,
    get_raw,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
)
from repositories.libraries.basedosdados.solids import upload_to_bigquery


@solid(required_resource_keys={"basedosdados_config", "timezone_config"},)
def pre_treatment_br_rj_riodejaneiro_onibus_gps(context, data, timestamp):

    timezone = context.resources.timezone_config["timezone"]

    context.log.info(f"data={data.json()}")
    data = data.json()
    df = pd.DataFrame(data)
    timestamp_captura = pd.to_datetime(timestamp)
    df["timestamp_captura"] = timestamp_captura
    # Remove timezone and force it to be config timezone
    df["datahora"] = (
        df["datahora"]
        .astype(float)
        .apply(
            lambda ms: pd.to_datetime(
                pendulum.from_timestamp(ms / 1000.0)
                .replace(tzinfo=None)
                .set(tz=timezone)
                .isoformat()
            )
        )
    )

    # Filter data for 0 <= time diff <= 1min
    try:
        datahora_col = "datahora"
        df_treated = df
        df_treated[datahora_col] = df_treated[datahora_col].apply(
            lambda x: x.tz_convert(timezone)
        )
        df_treated["timestamp_captura"] = df_treated["timestamp_captura"].apply(
            lambda x: x.tz_convert(timezone)
        )
        mask = (df_treated["timestamp_captura"] - df_treated[datahora_col]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=1)
        )
        df_treated = df_treated[mask]
        context.log.info(f"Shape antes da filtragem: {df.shape}")
        context.log.info(f"Shape após a filtrage: {df_treated.shape}")
        if df_treated.shape[0] == 0:
            raise ValueError("After filtering, the dataframe is empty!")
        df = df_treated
    except:
        err = traceback.format_exc()
        log_critical(f"Failed to filter STPL data: \n{err}")

    return df


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
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "250m", "memory": "500Mi"},
                    "limits": {"cpu": "1500m", "memory": "1Gi"},
                },
            }
        },
    },
)
def br_rj_riodejaneiro_onibus_gps_registros():

    filename, partitions = create_current_datetime_partition()
    file_path = get_file_path_and_partitions(filename, partitions)

    data, timestamp, error = get_raw()

    upload_logs_to_bq(timestamp, error)

    raw_file_path = save_raw_local(data, file_path)

    treated_data = pre_treatment_br_rj_riodejaneiro_onibus_gps(data, timestamp)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery([raw_file_path, treated_file_path], partitions)
