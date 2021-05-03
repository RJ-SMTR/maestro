from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    PresetDefinition,
)

from pathlib import Path

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
    upload_file_to_storage,
    parse_file_path_and_partitions, 
    save_treated_local,
)
from repositories.libraries.basedosdados.solids import (
    upload_to_bigquery,
)
from repositories.libraries.pandas.solids import (
    load_and_reindex_csv,
    add_timestamp,
)


@solid
def divide_columns(context, df, cols_to_divide=None, value=100):
    if cols_to_divide:
        # Divide columns by value
        df[cols_to_divide] = df[cols_to_divide].apply(lambda x: x/value, axis=1)
    return df


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
            config_files=[str(Path(__file__).parent / "rdo5_registros.yaml")],
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
    treated_data = load_and_reindex_csv(raw_file_path)
    treated_data = add_timestamp(treated_data)
    treated_data = divide_columns(treated_data)

    treated_file_path = save_treated_local(treated_data, file_path)

    upload_to_bigquery([treated_file_path], partitions)