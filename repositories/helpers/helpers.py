import importlib
import logging
import yaml
logger = logging.getLogger(__name__)


def read_config(yaml_file):
    with open(yaml_file, "r") as load_file:
        config = yaml.load(load_file, Loader=yaml.FullLoader)
        return config

def load_repository(filename: str):
    config = read_config(filename)
    repository_list = []
    for obj_type, modules in config.items():
        for item in modules:
            module = item["module"]
            function_list = item["objects"]
            repository_list += load_module(obj_type, module, function_list)
    return repository_list


def load_module(obj_type: str, module: str, function_list: list):
    repository_list = []
    print(f"Importing {obj_type}")
        # logger.info("LOG %s ", module)
    try:
        imported = importlib.import_module(module)
        print(f"Imported module {module}")
        for func in function_list:
            try:
                imported_func = getattr(imported, func)
                repository_list.append(imported_func)
                print(f"Imported {obj_type} {func}")
            except Exception:
                print(f"Could not import {obj_type} {func}")
                # logger.info("Imported %s %s", obj_type, item.__name__)
    except Exception as err:
        print(f"Could not import module {module}")
        # logger.error("Could not import module %s", module)
    return repository_list