from dagster import solid


@solid
def test_solid(context):

    print("hi")
