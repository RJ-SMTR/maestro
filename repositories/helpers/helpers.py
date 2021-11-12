import importlib
from typing import Iterable
import yaml
import traceback
from pathlib import Path
import os
from jinja2 import Environment, FileSystemLoader

from repositories.helpers.logging import logger


def map_dict_keys(data: dict, mapping: dict) -> None:
    pop_keys = [old_key for old_key in data.keys() if old_key not in mapping.keys()]
    for old_key, new_key in mapping.items():
        data[new_key] = data.pop(old_key)
    for key in pop_keys:
        data.pop(key)
    return None


def remove_duplicates(iterable: Iterable):
    """
    Remove duplicates from an iterable.
    Returns a generator.
    """
    seen = set()
    for item in iterable:
        if item not in seen:
            yield item
            seen.add(item)


def env_override(value, key):
    return os.getenv(key, value)


def read_config(yaml_file):

    logger.debug("Setting template folder as {}", Path(yaml_file).parent)
    file_loader = FileSystemLoader(Path(yaml_file).parent)
    env = Environment(loader=file_loader)
    env.filters["env_override"] = env_override

    logger.debug("Setting template as {}", os.path.basename(yaml_file))
    template = env.get_template(os.path.basename(yaml_file))

    output = template.render()

    config = yaml.load(output, Loader=yaml.FullLoader)
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
            except Exception as err:
                logger.info(f"Could not import {obj_type} {func}")
                logger.info(traceback.print_tb(err.__traceback__))
                logger.info(err)

    except Exception as err:
        logger.info(f"Could not import module {module}")
        logger.info(traceback.print_tb(err.__traceback__))
        logger.info(err)

    return repository_list


def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except ValueError:
        return default
