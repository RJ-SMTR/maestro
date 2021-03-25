from dagster import solid, pipeline

from repositories.modules.dryrun.solids import test_solid


@pipeline
def test_pipeline():

    test_solid()