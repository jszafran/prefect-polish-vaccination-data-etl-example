from typing import List, Optional
from urllib.parse import urlparse

import requests


def get_resources_page_links() -> List[str]:
    """
    Fetch available resource pages from dane.gov.pl API.
    """

    def extract_page_num_from_query(query: str) -> Optional[int]:
        if "page=" not in query:
            return None
        return int(query.split("=")[1])

    url = "https://api.dane.gov.pl/1.4/datasets/2476,odsetek-osob-zaszczepionych-przeciwko-covid19-w-gm/resources"
    response = requests.get(url)
    response.raise_for_status()
    links = response.json()["links"]
    first_link, last_link = urlparse(links["self"]), urlparse(links["last"])
    first_page_num, last_page_num = (
        extract_page_num_from_query(first_link.query),
        extract_page_num_from_query(last_link.query),
    )

    return [
        f"{url}?page={page_num}"
        for page_num in range(first_page_num, last_page_num + 1)
    ]
