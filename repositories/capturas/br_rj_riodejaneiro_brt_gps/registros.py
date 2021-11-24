import traceback
from datetime import timedelta

import requests
import pendulum
import pandas as pd
from dagster import (
    solid,
    pipeline,
    ModeDefinition,
)
from dagster.core.definitions.events import Output
from dagster.core.definitions.output import OutputDefinition

from repositories.capturas.resources import (
    keepalive_key,
    mapping,
    timezone_config,
    discord_webhook,
)
from repositories.helpers.constants import constants
from repositories.helpers.datetime import convert_unix_time_to_datetime
from repositories.helpers.helpers import safe_cast, map_dict_keys
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
from repositories.libraries.basedosdados.solids import bq_upload, upload_to_bigquery



@solid(
    required_resource_keys={"basedosdados_config", "timezone_config", 'mapping'},
    output_defs=[
        OutputDefinition(name="treated_data", is_required=True),
        OutputDefinition(name="error", is_required=False)],
)
def pre_treatment_br_rj_riodejaneiro_brt_gps(context, data, timestamp, key_column, prev_error):

    columns = [key_column,"timestamp_gps", "timestamp_captura", "content"]

    context.log.info(f"Previous error is {prev_error}")

    if prev_error is not None:
        yield Output(pd.DataFrame(), output_name="treated_data")
        yield Output(prev_error, output_name="error")
        return

    error = None
    timezone = context.resources.timezone_config["timezone"]

    data = data.json()
    df = pd.DataFrame(columns=columns)
    timestamp_captura = pd.to_datetime(timestamp).tz_convert(timezone)
    context.log.info(f"Timestamp captura is {timestamp_captura}")
    # map_dict_keys change data keys to match project data structure
    df["content"] = [map_dict_keys(piece, context.resources.mapping['map']) for piece in data]
    df[key_column] = [piece[key_column] for piece in data]
    df["timestamp_captura"] = timestamp_captura
    df["timestamp_gps"] = df["content"].apply(
            lambda x: pd.to_datetime(convert_unix_time_to_datetime(safe_cast(x["timestamp_gps"], float, 0))).tz_localize(
                timezone
            ).tz_convert(timezone)
        )
    context.log.info(f"Timestamp GPS is {df['timestamp_gps']}")
    # Filter data for 0 <= time diff <= 1min
    try:
        context.log.info(f"Shape antes da filtragem: {df.shape}")
        # df["timestamp_gps"] = df["content"].apply(
        #     lambda x: pd.to_datetime(convert_unix_time_to_datetime(safe_cast(x["timestamp_gps"], float, 0))).tz_localize(
        #         timezone
        #     )
        # )
        # context.log.info(f'Timestamp GPS is {df["timestamp_gps"]}')
        mask = (df["timestamp_captura"] - df["timestamp_gps"]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=1)
        )
        df = df[mask]
        df = df[columns]
        if df.shape[0] == 0:
            raise ValueError("After filtering, the dataframe is empty!")
    except Exception as e:
        err = traceback.format_exc()
        log_critical(f"Failed to filter BRT data: \n{err}")
        df = pd.DataFrame(columns=columns)
        error = e

    context.log.info(f"Error now is {error}")
    yield Output(df, output_name="treated_data")
    yield Output(error, output_name="error")


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
                "mapping": mapping
            },
        ),
    ],
    tags={
        "pipeline": "br_rj_riodejaneiro_brt_gps_registros",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "300m", "memory": "250Mi"},
                    "limits": {"cpu": "600m", "memory": "500Mi"},
                },
            }
        },
    },
)
def br_rj_riodejaneiro_brt_gps_registros():

    filename, partitions = create_current_datetime_partition()
    file_path = get_file_path_and_partitions(filename, partitions)

    data, timestamp, error = get_raw()

    raw_file_path = save_raw_local(data, file_path)

    treated_data, error = pre_treatment_br_rj_riodejaneiro_brt_gps(
        data, timestamp, prev_error=error)

    upload_logs_to_bq(timestamp, error)

    treated_file_path = save_treated_local(treated_data, file_path)

    bq_upload(treated_file_path, raw_filepath=raw_file_path, partitions=partitions)
