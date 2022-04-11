import pathlib

from prefect import Flow
from prefect.executors import LocalExecutor
from prefect.run_configs import LocalRun

from src.tasks import (
    download_csv,
    get_csv_links_from_resource,
    get_resources_page_links,
)

with Flow(
    "COVID19 vaccination rate for Poland flow",
    run_config=LocalRun(env={"foo": "bar"}),
    executor=LocalExecutor(),
) as flow:
    resources_links = get_resources_page_links()
    csv_links = get_csv_links_from_resource(resources_links[0])
    resource_metadata = csv_links[0]
    download_csv(
        resource_metadata=resource_metadata,
        target_dir=pathlib.Path(__file__).parent / "data",
    )


flow.register(project_name="sandbox")
