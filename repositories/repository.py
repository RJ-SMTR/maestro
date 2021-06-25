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
def analises():
    repository_list = load_repository(
        Path(__file__).parent / "repository.yaml", "analises"
    return repository_list


@repository
def queries():
    repository_list = load_repository(
        Path(__file__).parent / "repository.yaml", "queries"
    )
    return repository_list


# @repository
# def examples():
#     repository_list = load_repository(
#         Path(__file__).parent / "repository.yaml", "examples"
#     )
#     return repository_list
