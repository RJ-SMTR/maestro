from enum import Enum
from os import getenv


class constants(Enum):
    """
    Constants for the repositories
    """

    REDIS_HOST = "dagster-redis-master"
    MAESTRO_REPOSITORY = "RJ-SMTR/maestro"
    MAESTRO_DEFAULT_BRANCH = "main"
    MAESTRO_BQ_REPOSITORY = "RJ-SMTR/maestro-bq"
    MAESTRO_BQ_DEFAULT_BRANCH = "master"
    CRITICAL_DISCORD_WEBHOOK = getenv("CRITICAL_DISCORD_WEBHOOK", "")
