from enum import Enum
from os import getenv
from datetime import datetime


class constants(Enum):
    """
    Constants for the repositories
    """

    SECOND = 1
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    MONTH = 30 * DAY
    YEAR = 365 * DAY
    SECOND_TO_MS = 1000

    REDIS_HOST = getenv("REDIS_HOST", "dagster-redis-master")
    REDIS_LOCK_AUTO_RELEASE_TIME = 12 * HOUR * SECOND_TO_MS
    REDIS_KEY_MAT_VIEWS_BLOBS_SET = "materialized_views_files_set"
    REDIS_KEY_MAT_VIEWS_LAST_RUN_MTIME = "materialized_views_last_run_mtime"
    REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS = "managed_materialized_views"
    REDIS_KEY_MAT_VIEWS_MANAGED_VIEWS_LOCK = "lock_managed_materialized_views"
    REDIS_KEY_MAT_VIEWS_MATERIALIZE_LOCK = "lock_materialize_materialized_views"
    REDIS_KEY_MAT_VIEWS_MATERIALIZE_SENSOR_LOCK = "lock_materialize_materialized_views_sensor"
    REDIS_MATERIALIZED_VIEW_DEFAULT_DICT_KEYS = [
        "cron_expression", "last_run", "materialized", "query_modified", "depends_on"]
    REDIS_MATERIALIZED_VIEW_DEFAULT_DICT_TYPES = [
        str, datetime, bool, bool, list]
    MAESTRO_REPOSITORY = "RJ-SMTR/maestro"
    MAESTRO_DEFAULT_BRANCH = "main"
    MAESTRO_BQ_REPOSITORY = "RJ-SMTR/maestro-bq"
    MAESTRO_BQ_DEFAULT_BRANCH = "master"
    CRITICAL_DISCORD_WEBHOOK = getenv("CRITICAL_DISCORD_WEBHOOK", "")
    SIGMOB_GET_REQUESTS_TIMEOUT = 3600  # 1 hour
