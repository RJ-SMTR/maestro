from dagster import (
    pipeline,
    ModeDefinition,
    SolidInvocation,
    PipelineDefinition,
    PresetDefinition,
    DependencyDefinition,
    InputDefinition,
    Nothing,
)

from repositories.libraries.basedosdados.solids import render_and_create_view
from repositories.libraries.basedosdados.resources import bd_client

import yaml


# @pipeline(mode_defs=[ModeDefinition(resource_defs={"bd_client": bd_client})])
# def test_pipeline():

#     q1 = render_and_create_view.alias('first_query')()
#     q2 = render_and_create_view.alias('second_query')()


def test_pipeline():
    return construct_pipeline_with_yaml(
        "/Users/joaoc/Documents/Projects/smtr/maestro/repositories/treatments/normalize_gps/pipeline.yaml",
        [render_and_create_view],
    )


def construct_pipeline_with_yaml(yaml_file, solid_defs):
    yaml_data = yaml.load(open(yaml_file, "r").read())
    solid_def_dict = {s.name: s for s in solid_defs}

    deps = {}
    config = {"solids": {}}

    for solid_yaml_data in yaml_data["pipeline"]["steps"]:
        # check.invariant(solid_yaml_data["def"] in solid_def_dict)

        def_name = solid_yaml_data["uses_solid"]
        alias = solid_yaml_data.get("name", def_name)

        # Solve Dependencies
        solid_deps_entry = {}

        for input_name, input_data in solid_yaml_data.get("depends_on", {}).items():

            solid_deps_entry[input_name] = DependencyDefinition(
                solid=input_data, output="result"
            )

        deps[SolidInvocation(name=def_name, alias=alias)] = solid_deps_entry

        # Solve Config
        config["solids"][alias] = {"config": solid_yaml_data.get("config")}

    return PipelineDefinition(
        name=yaml_data["pipeline"]["name"],
        description=yaml_data["pipeline"].get("description"),
        solid_defs=solid_defs,
        dependencies=deps,
        mode_defs=[ModeDefinition(resource_defs={"bd_client": bd_client})],
        preset_defs=[PresetDefinition(name="default", run_config=config)],
    )
