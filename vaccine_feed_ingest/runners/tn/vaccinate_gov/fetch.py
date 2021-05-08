#!/usr/bin/env python

import asyncio
import json
import pathlib
import sys
from urllib.parse import urljoin

from aiohttp import ClientSession
from bs4 import BeautifulSoup

start_url = "https://vaccinate.tn.gov/vaccine-centers/"


async def parse_start_page(output_dir, session):
    result = {}
    output_file = output_dir / "vaccinate_gov.html"
    async with session.get(start_url) as response:
        contents = await response.text()
        with output_file.open("w") as f:
            f.write(contents)
            f.write("\n")
        doc = BeautifulSoup(contents, "html.parser")

        csrf_url_el = doc.find(id="antiforgerytoken")
        if csrf_url_el is None:
            raise Exception(
                """Couldn't find element with id 'antiforgerytoken' for csrf
                token endpoint.  Maybe the id has changed?  The endpoint may
                look like: /_layout/tokenhtml"""
            )
        # combine base url and relative or absolute href to produce a new url
        result["csrf_url"] = urljoin(start_url, csrf_url_el.attrs["data-url"])

        map_el = doc.find(id="entity-list-map")
        if map_el is None:
            raise Exception(
                """Couldn't find an element with id 'entity-list-map'. This
                element used to contain several items of data needed for the
                search request: data-search-url, lat, long, units, data-entity-list-id"""
            )
        # includes data-search-url and more
        for k, v in map_el.attrs.items():
            if k.startswith("data-"):
                result[k] = v
        result["search_url"] = urljoin(start_url, map_el.attrs["data-search-url"])

        return result


async def get_csrf_token(session, csrf_url):
    async with session.get(csrf_url) as response:
        contents = await response.text()
        doc = BeautifulSoup(contents, "html.parser")
        token_el = doc.find("input", attrs={"name": "__RequestVerificationToken"})
        if token_el is None:
            raise Exception(
                "Couldn't find input element with name __RequestVerificationToken."
            )
        return token_el.attrs["value"]


async def get_locations_raw(session, start_page, csrf_token):
    headers = {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "__RequestVerificationToken": csrf_token,
    }
    payload = json.dumps(
        {
            "longitude": start_page["data-longitude"],
            "latitude": start_page["data-latitude"],
            # This distance is a hand picked value that ensures we cover
            # results from the entire state
            "distance": 500,
            "units": start_page["data-distance-units"],
            "id": start_page["data-entity-list-id"],
        }
    )
    search_url = start_page["search_url"]
    async with session.post(search_url, data=payload, headers=headers) as response:
        raw = await response.text()
        return raw


async def main(argv):
    output_dir = pathlib.Path(argv[0])
    output_file = output_dir / "locations.json"

    async with ClientSession() as session:
        start_page = await parse_start_page(output_dir, session)
        csrf_token = await get_csrf_token(session, start_page["csrf_url"])
        raw = await get_locations_raw(session, start_page, csrf_token)
        with output_file.open("w") as f:
            f.write(raw)
            f.write("\n")


# If this file is being run from the CLI instead of imported as a module
if __name__ == "__main__":
    # discard the first item in sys.argv as it's the script name.
    # Example: '.../fetch.py'
    argv = sys.argv[1:]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(argv))
