from dagster import repository
from repositories.helpers.helpers import load_repository
from pathlib import Path


@repository
def capturas():
    repository_list = load_repository(
        Path(__file__).parent / "repository.yaml", "capturas"
    )
    return repository_list


@repository
def treatments():
    repository_list = load_repository(
        Path(__file__).parent / "repository.yaml", "treatments"
    )
    return repository_list
