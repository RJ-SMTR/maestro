import importlib
import yaml
import traceback
from pathlib import Path
import os
from jinja2 import Environment, FileSystemLoader

from dagster import (
    SolidInvocation,
    PipelineDefinition,
    PresetDefinition,
    DependencyDefinition,
    Nothing,
)


from repositories.helpers.logging import logger

def env_override(value, key):
    return os.getenv(key, value)

def read_config(yaml_file):

    logger.debug("Setting template folder as {}", Path(yaml_file).parent)
    file_loader = FileSystemLoader(Path(yaml_file).parent)
    env = Environment(loader=file_loader)
    env.filters['env_override'] = env_override

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
                if imported_func.__class__.__name__ == "function":
                    imported_func = imported_func()
                repository_list.append(imported_func)
                logger.info(f"Imported {obj_type} {func}")
            except Exception as err:
                logger.info(f"Could not import {obj_type} {func}")
                traceback.print_tb(err.__traceback__)
                print(err)

    except Exception as err:
        logger.info(f"Could not import module {module}")
        traceback.print_tb(err.__traceback__)
        print(err)

    return repository_list


def construct_pipeline_with_yaml(yaml_file, pipeline_kwargs):

    yaml_data = yaml.load(open(yaml_file, "r").read())

    dependencies = {}
    solid_defs = []

    for step in yaml_data["pipeline"]["steps"]:

        module, solid_name = step["uses_solid"].rsplit(".", 1)
        solid_alias = step.get("alias", solid_name)

        # Solve Dependencies
        solid_deps_entry = {}
        for input_name, input_data in step.get("depends_on", {}).items():

            solid_deps_entry[input_name] = DependencyDefinition(
                solid=input_data, output="result"
            )
        dependencies[
            SolidInvocation(name=solid_name, alias=solid_alias)
        ] = solid_deps_entry

        # Add solid_sefs
        solid_defs.append(getattr(importlib.import_module(module), solid_name))

    # Solve preset_defs
    preset_defs = []
    for preset in yaml_data["pipeline"]["presets"]:

        run_config = {"solids": {}}

        for step in yaml_data["pipeline"]["steps"]:

            solid_alias = step.get("alias", solid_name)
            run_config["solids"][solid_alias] = {
                "config": step.get("config").get(preset["name"])
            }

        preset_defs.append(
            PresetDefinition(
                name=preset.get("name"),
                mode=preset.get("mode"),
                tags=preset.get("tags"),
                run_config=run_config,
            )
        )

    return PipelineDefinition(
        solid_defs=list(set(solid_defs)),
        name=yaml_data["pipeline"]["name"],
        description=yaml_data["pipeline"].get("description"),
        dependencies=dependencies,
        preset_defs=preset_defs,
        tags=yaml_data["pipeline"].get("tags"),
        **pipeline_kwargs,
    )
