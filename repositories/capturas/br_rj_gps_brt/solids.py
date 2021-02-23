from dagster import solid, pipeline

import requests


@solid
def get_data(context):

    context.log.info("Fetching url")

    content = requests.get("http://webapibrt.rio.rj.gov.br/api/v1/brt")

    return content.text


@solid
def save_data(context, content):

    context.log.info("Saving content")

    open(f"data/{str(context.run_id)}.json", "w").write(content)


@pipeline
def hello_world_pipeline():
    save_data(get_data())