from dagster import resource, Field


@resource(
    {"date": Field(str, is_required=True, description="Date partition from schedule")}
)
def schedule_run_date(context):
    return context.resource_config


@resource(
    {
        "from": Field(str, is_required=True),
        "password": Field(str, is_required=True),
        "to": Field(str, is_required=True),
        "subject": Field(str, is_required=False),
        "content": Field(
            list,
            is_required=True,
            description="Conteúdo do email a ser enviado. Cada elemento da lista corresponde à uma linha no corpo da mensagem",
        ),
    }
)
def automail_config(context):
    return context.resource_config
