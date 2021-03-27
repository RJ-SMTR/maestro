from dagster import (
    solid,
    resource,
    Field,
    InputDefinition,
    OutputDefinition,
    Nothing,
)
from jinja2 import Template


@solid(
    input_defs=[InputDefinition("_", Nothing)],
    output_defs=[OutputDefinition(str)],
    config_schema={"filepath": str, "context": Field(dict, is_required=True)},
)
def jinja2_render(context):

    result = Template(open(context.solid_config["filepath"], "r").read()).render(
        context.solid_config["context"]
    )

    context.log.info("\n" + result)

    return result
