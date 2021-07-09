from dagster import resource, Field

from basedosdados.upload.base import Base


@resource
def bd_client(context):
    return Base().client["bigquery_prod"]


@resource(
    {
        "table_id": Field(
            str, is_required=True, description="Table used in the pipeline"
        ),
        "dataset_id": Field(
            str, is_required=True, description="Dataset used in the pipeline"
        ),
    }
)
def basedosdados_config(context):
    return context.resource_config