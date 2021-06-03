#!/usr/bin/env python3

import asyncio
import logging
import pathlib
import sys

from aiohttp import ClientSession

output_dir = pathlib.Path(sys.argv[1])
base_url = "https://labtools.curativeinc.com/api/v1/testing_sites"


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("curative")


# def fetch_ids():
#     # fetching the base url returns a list of sites and their ids
#     #make the request
#     r = http.request("GET", base_url)
#     #get the response and decode it, assuming utf8
#     obj = json.loads(r.data.decode("utf-8"))

#     valid_ids = [o.get("id") for o in obj]
#     # valid_ids.sort()
#     return valid_ids


def generate_url_for(site_id):
    """
    generate and return a tuple containing the next curative url and the output destination for that file
    """
    url = f"{base_url}/{site_id}"
    output_file = output_dir / f"{site_id}.json"
    return (url, output_file)


async def fetch_location(session: ClientSession, site_id: int) -> None:
    url, output_file = generate_url_for(site_id)
    logger.info("getching site id: " + str(site_id))
    async with session.get(url) as response:
        contents = await response.text()
        with output_file.open("w") as f:
            f.write(contents)
            f.write("\n")


async def main(argv):
    async with ClientSession() as session:
        # fetch the baseurl to get a list of ids
        async with session.get(base_url) as response:
            contents = await response.json()

            valid_ids = [o.get("id") for o in contents]
            # valid_ids.sort()

            futures = [fetch_location(session, site_id) for site_id in valid_ids]
            await asyncio.gather(*futures)


# If this file is being run from the CLI instead of imported as a module
if __name__ == "__main__":
    # discard the first item in sys.argv as it's the script name.
    # Example: '.../fetch.py'
    argv = sys.argv

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(argv))
