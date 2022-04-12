from prefect import Flow, Parameter, flatten, unmapped

from tasks import (
    download_csv,
    get_csv_links_from_resource_page,
    get_resources_page_links,
)

# define flow
with Flow(
    "COVID19 vaccination rate for Poland flow"
) as covid19_vaccination_rate_etl_flow:
    # extract all available resource pages
    # (each resource page contains multiple links for CSV storing daily vaccination rate data)
    resources_links = get_resources_page_links()

    # extract csv links from resource pages
    csv_links = get_csv_links_from_resource_page.map(resources_links)

    # flatten csv links and download them all to hard drive
    download_csv.map(
        resource_metadata=flatten(csv_links),
        target_dir=unmapped(Parameter("data_dir")),
    )
