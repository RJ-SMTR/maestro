import importlib
import logging
import yaml
import traceback


from repositories.helpers.logging import logger


def read_config(yaml_file):
    with open(yaml_file, "r") as load_file:
        config = yaml.load(load_file, Loader=yaml.FullLoader)
        return config


def load_repository(filename: str, repository_name: str):
    config = read_config(filename)[repository_name]
    repository_list = []
    for obj_type, modules in config.items():
        for item in modules:
            module = item["module"]
            function_list = item["objects"]
            repository_list += load_module(obj_type, module, function_list)
    return repository_list


def load_module(obj_type: str, module: str, function_list: list):
    repository_list = []
    logger.info("Trying {} ", module)
    try:
        imported = importlib.import_module(module)
        logger.info(f"Imported module {module}")
        for func in function_list:
            try:
                imported_func = getattr(imported, func)
                repository_list.append(imported_func)
                logger.info(f"Imported {obj_type} {func}")
            except Exception:
                logger.info(f"Could not import {obj_type} {func}")

    except Exception as err:
        logger.info(f"Could not import module {module}")
        traceback.print_tb(err.__traceback__)

    return repository_list