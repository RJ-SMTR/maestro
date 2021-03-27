from dagster import resource, Field

from basedosdados.base import Base


@resource
def bd_client(context):
    return Base().client["bigquery_prod"]