from repositories.helpers.io import get_blob
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
import io
import zipfile
import gtfs_kit as gk
from datetime import datetime
from pathlib import Path
import rgtfs

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
    get_file_path_and_partitions,
    upload_blob_to_storage,
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
def create_gtfs_version_partition(context, feed, original_filepath, bucket_name):
    # If feed_info.txt is available, use GTFS version as partition
    if feed.feed_info is not None:
        version_partition = feed.feed_info('feed_version')
    # Otherwise, use txt modification date
    else:
        blob = get_blob(original_filepath, bucket_name)
        single_file = zipfile.ZipFile(io.BytesIO(
            blob.download_as_bytes()), 'r').infolist()[0]
        version_partition = datetime(*single_file.date_time).strftime("%Y%m%d")

    partitions = f"gtfs_version_date={version_partition}"
    context.log.debug(f"Creating partition {partitions}")

    return partitions


@solid
def pre_treatment_br_rj_riodejaneiro_gtfs_planned(context, feed, file):
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
    context.log.debug(f"File path to add to list: {treated_file_path}")
    return [treated_file_path]


@composite_solid
def process_gtfs_files(feed, partitions, file):
    treated_data = pre_treatment_br_rj_riodejaneiro_gtfs_planned(feed, file)
    gtfs_file_path = get_file_path_and_partitions(
        filename=file, partitions=partitions, table_id=file)
    gtfs_treated_file_path = save_treated_local(treated_data, gtfs_file_path)
    gtfs_treated_file_path_list = process_filename(gtfs_treated_file_path)
    upload_to_bigquery(file_paths=gtfs_treated_file_path_list,
                       partitions=partitions, table_id=file)


@solid
def get_realized_trips(file_path):
    realized_trips = rgtfs.generate_realized_trips_from_gtfs(file_path)
    return realized_trips


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
    preset_defs=[
        PresetDefinition.from_files(
            "BRT_GTFS",
            config_files=[str(Path(__file__).parent / "gtfs_planned.yaml")],
            mode="dev",
        ),
    ],
    # tags={"dagster/priority": "-1"}
)
def br_rj_riodejaneiro_gtfs_planned_feed():

    # TODO: Adapt this pipeline for GCS
    feed = open_gtfs_feed()
    partitions = create_gtfs_version_partition(feed=feed)
    upload_blob_to_storage(partitions=partitions)

    # GTFS
    files = get_gtfs_files()
    files.map(lambda file: process_gtfs_files(
        feed=feed, file=file, partitions=partitions))

    # Realized trips
    realized_file_path = get_file_path_and_partitions.alias(
        'realized_get_file_path_and_partitions')(partitions=partitions)
    realized_trips = get_realized_trips()
    realized_treated_file_path = save_treated_local.alias('realized_save_treated_local')(
        realized_trips,
        realized_file_path)
    realized_treated_file_path_list = process_filename.alias('realized_process_filename')(
        realized_treated_file_path)
    upload_to_bigquery.alias('realized_upload_to_bigquery')(
        realized_treated_file_path_list,
        partitions)
