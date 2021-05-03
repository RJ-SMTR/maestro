from dagster import (
    solid,
    pipeline,
    ModeDefinition,
    solid,
    composite_solid,
    PresetDefinition,
)
from dagster.experimental import (
    DynamicOutput,
    DynamicOutputDefinition,
)
import zipfile
import gtfs_kit as gk
from datetime import datetime
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
    get_file_path_and_partitions, 
    upload_file_to_storage,
    save_treated_local,
)

from repositories.libraries.basedosdados.solids import (
    upload_to_bigquery,
)

@solid
def open_gtfs_feed(context, original_filepath):
    context.log.debug(f"Opening GTFS {original_filepath}")
    feed = gk.read_feed(original_filepath, dist_units='km')

    return feed

@solid(
    output_defs=[
        DynamicOutputDefinition(name="filename"),
    ],
)
def get_gtfs_files(context, original_filepath):
    feed_files = gk.list_feed(original_filepath)['file_name']
    for item in feed_files:
        filename = Path(item).stem
        yield DynamicOutput(filename, mapping_key=filename, 
                            output_name='filename')

@solid
def create_gtfs_version_partition(context, feed, original_filepath):
    # If feed_info.txt is available, use GTFS version as partition
    if feed.feed_info is not None:
        version_partition = feed.feed_info('feed_version')
    # Otherwise, use txt modification date
    else:
        single_file = zipfile.ZipFile(original_filepath, 'r').infolist()[0]
        version_partition = datetime(*single_file.date_time).strftime("%Y%m%d")

    partitions = f"gtfs_version_date={version_partition}"
    context.log.debug(f"Creating partition {partitions}")

    return partitions

@solid
def pre_treatment_br_rj_riodejaneiro_gtfs(context, feed, file):
    context.log.debug(f"Getting file {file}")
    # Get dataframe
    raw_data = getattr(feed, file)

    # Add extra columns
    try:
        reindex_kwargs = context.solid_config[file] 
        treated_data = raw_data.reindex(**reindex_kwargs)
    except KeyError:
        treated_data = raw_data
    return treated_data


@solid
def process_filename(context, treated_file_path):
    return [treated_file_path]


@composite_solid
def process_gtfs_files(feed, partitions, file):
    treated_data = pre_treatment_br_rj_riodejaneiro_gtfs(feed, file)
    file_path = get_file_path_and_partitions(filename=file, partitions=partitions, table_id=file)
    treated_file_path = save_treated_local(treated_data, file_path)
    treated_file_path_list = process_filename(treated_file_path)
    upload_to_bigquery(file_paths=treated_file_path_list, partitions=partitions, table_id=file)


@discord_message_on_failure
@discord_message_on_success
@pipeline(
    mode_defs=[
        ModeDefinition(
            "dev", resource_defs={"basedosdados_config": basedosdados_config, 
                                  "timezone_config": timezone_config,
                                  "discord_webhook": discord_webhook}
        ),
    ],
    preset_defs=[
        PresetDefinition.from_files(
            "BRT_GTFS",
            config_files=[str(Path(__file__).parent / "brt_gtfs.yaml")],
            mode="dev",
        ),
    ],
)
def br_rj_riodejaneiro_gtfs_feed():

    feed = open_gtfs_feed()
    partitions = create_gtfs_version_partition(feed=feed)
    upload_file_to_storage(partitions=partitions)

    files = get_gtfs_files()
    files.map(lambda file: process_gtfs_files(
        feed=feed, file=file, partitions=partitions))