#!/usr/bin/env python

import asyncio
import pathlib
import sys
from typing import List
from urllib.parse import urljoin

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)
start_url = "https://dph.georgia.gov/locations/covid-vaccination-site"


def parse_location_links(doc: BeautifulSoup) -> List[str]:
    """Returns a list of full urls to individual location pages"""
    datatable = doc.find(id="datatable")
    if datatable is None:
        logger.warn(
            "datatable not found, individual location pages will not be fetched"
        )
        return []
    anchors = datatable.find_all("a")
    urls: List[str] = []
    for a in anchors:
        href = a.attrs["href"]
        if a.attrs["href"] is None:
            continue
        # combine the starting url and the relative or absolute href to produce
        # a new url
        url = urljoin(start_url, href)
        urls.append(url)
    return urls


def location_file_name_for_url(url: str) -> str:
    return "location-" + url.split("/")[-1] + ".html"


async def fetch_location(
    session: ClientSession, output_dir: pathlib.Path, url: str
) -> None:
    """
    Downloads the html for a location url and writes to a file
    'location-{slug}.html'
    """
    file_name = location_file_name_for_url(url)
    output_file = output_dir / file_name

    async with session.get(url) as response:
        contents = await response.text()
        with output_file.open("w") as f:
            f.write(contents)
            f.write("\n")


async def main():
    output_dir = pathlib.Path(sys.argv[1])
    output_file = output_dir / "locations.html"

    logger.info("starting")

    async with ClientSession() as session:
        contents = ""
        async with session.get(start_url) as response:
            contents = await response.text()
            with output_file.open("w") as f:
                f.write(contents)
                f.write("\n")

        doc = BeautifulSoup(contents, "html.parser")
        location_links = parse_location_links(doc)
        futures = [fetch_location(session, output_dir, url) for url in location_links]
        await asyncio.gather(*futures)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
