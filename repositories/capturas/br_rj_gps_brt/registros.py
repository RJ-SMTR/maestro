from dagster import solid, pipeline

import requests


@solid
def get_data(context):

    context.log.info("Fetching url")

    content = requests.get("http://webapibrt.rio.rj.gov.br/api/v1/brt")

    return content.text


@solid
def save_raw(context, content):

    context.log.info("Saving content")

    open(f"data/raw/{str(context.run_id)}.json", "w").write(content)


@solid
def save_staging(context, content):

    context.log.info("Saving content")

    open(f"data/staging/{str(context.run_id)}.json", "w").write(content)


@solid
def process_data(context, content):

    return content


@pipeline
def br_rj_gps_brt_registros():

    content = get_data()

    save_raw(content)

    processed_content = process_data(content)

    save_staging(processed_content)
