from dagster import (
    ModeDefinition,
)

from repositories.libraries.basedosdados.resources import bd_client
from repositories.helpers.helpers import construct_pipeline_with_yaml

import yaml

mode_defs = [
    ModeDefinition(name="default", resource_defs={"bd_client": bd_client}),
    ModeDefinition(name="dev", resource_defs={"bd_client": bd_client}),
]

hook_defs = set()

# Pipeline kwargs that are not infered from pipeline,yaml
# Currently, you can use: mode_defs, hook_defs.
# Ref: https://docs.dagster.io/_apidocs/pipeline#dagster.PipelineDefinition
pipeline_kwargs = dict(
    mode_defs=mode_defs,
    hook_defs=hook_defs,
)

# You have to use a function to call the pipeline constructor.
# It is a good practice to name it the same as the pipeline.
# This function has to be called in `repository.yaml`.
def example():
    return construct_pipeline_with_yaml(
        "repositories/examples/pipeline_yaml/pipeline.yaml",
        pipeline_kwargs=pipeline_kwargs,
    )
