import pathlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import prefect
import requests
from prefect import task


@dataclass(frozen=True)
class ResourceMetadata:
    csv_url: str
    date: datetime.date


@task(max_retries=3, retry_delay=timedelta(seconds=5))
def get_resources_page_links() -> List[str]:
    """
    Fetch available resource pages from dane.gov.pl API.
    """

    def extract_page_num_from_query(query: str) -> Optional[int]:
        if "page=" not in query:
            return None
        return int(query.split("=")[1])

    logger = prefect.context.get("logger")

    url = "https://api.dane.gov.pl/1.4/datasets/2476,odsetek-osob-zaszczepionych-przeciwko-covid19-w-gm/resources"
    response = requests.get(url, timeout=3)
    response.raise_for_status()
    links = response.json()["links"]
    first_link, last_link = urlparse(links["self"]), urlparse(links["last"])
    first_page_num, last_page_num = (
        extract_page_num_from_query(first_link.query),
        extract_page_num_from_query(last_link.query),
    )
    page_links = [
        f"{url}?page={page_num}"
        for page_num in range(first_page_num, last_page_num + 1)
    ]
    logger.info(f"Extracted {len(page_links)} pages: {page_links}")
    return page_links


@task(max_retries=3, retry_delay=timedelta(seconds=5))
def get_csv_links_from_resource_page(resource_url: str) -> List[ResourceMetadata]:
    def extract_metadata(data: Dict[str, Any]) -> ResourceMetadata:
        attributes = data["attributes"]
        return ResourceMetadata(
            csv_url=attributes["link"],
            date=datetime.strptime(attributes["data_date"], "%Y-%m-%d").date(),
        )

    response = requests.get(resource_url, timeout=3)
    response.raise_for_status()
    return [extract_metadata(daily_data) for daily_data in response.json()["data"]]


@task(max_retries=5, retry_delay=timedelta(seconds=5))
def download_csv(resource_metadata: ResourceMetadata, target_dir: pathlib.Path) -> None:
    target_path = target_dir / f"{resource_metadata.date.isoformat()}.csv"

    with open(str(target_path.absolute()), "wb") as f:
        f.write(requests.get(resource_metadata.csv_url, timeout=8).content)
