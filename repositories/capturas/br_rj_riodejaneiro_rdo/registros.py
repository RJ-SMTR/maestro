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

import basedosdados as bd

from repositories.capturas.resources import (
    timezone_config,
    discord_webhook,
)
from repositories.libraries.basedosdados.resources import (
    basedosdados_config,
)
from repositories.helpers.hooks import discord_message_on_failure, discord_message_on_success
from repositories.capturas.solids import (
    get_file_from_storage,
    parse_file_path_and_partitions, 
    save_treated_local,
)
from repositories.libraries.basedosdados.solids import (
    upload_to_bigquery,
)


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


@solid(config_schema={"delimiter": str, "header_lines": int})
def process_csv(context, file_path, original_header, column_mapping, ordered_header):
    
    delimiter = context.solid_config["delimiter"] 
    header_lines = context.solid_config["header_lines"]

    # Rearrange columns
    df = pd.read_csv(file_path, delimiter=delimiter, 
                     skiprows=header_lines,
                     names=original_header,
                     index_col=False)
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
def divide_columns(context, df, cols_to_divide=None, value=100):
    if cols_to_divide:
        # Divide columns by value
        df[cols_to_divide] = df[cols_to_divide].apply(lambda x: x/value, axis=1)
    return df

@solid(
    required_resource_keys={"basedosdados_config"},
)
def upload_file_to_storage(context, file_path, partitions, mode='raw'):

    # Upload to storage
    table_id = context.resources.basedosdados_config['table_id']
    dataset_id = context.resources.basedosdados_config['dataset_id']

    st = bd.Storage(table_id=table_id, dataset_id=dataset_id)

    context.log.debug(f"Uploading file {file_path} to mode {mode} with partitions {partitions}")
    st.upload(path=file_path, mode=mode, partitions=partitions, if_exists='replace')

    return True


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    preset_defs=[
        PresetDefinition.from_files(
            "BRT_RDO_40",
            config_files=[str(Path(__file__).parent / "brt_rdo40_registros.yaml")],
            mode="dev",
        ),
        PresetDefinition.from_files(
            "Onibus_RDO_40",
            config_files=[str(Path(__file__).parent / "onibus_rdo40_registros.yaml")],
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

    uploaded = upload_file_to_storage(partitions=partitions)
    raw_file_path = get_file_from_storage(file_path=file_path, filename=filename, 
                                          partitions=partitions, filetype=filetype,
                                          uploaded=uploaded)

    # Extract, load and transform
    original_header, column_mapping, ordered_header = get_headers()
    treated_data = process_csv(raw_file_path, original_header, column_mapping, ordered_header)
    treated_data = add_timestamp(treated_data)
    treated_data = divide_columns(treated_data)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery([treated_file_path], partitions)