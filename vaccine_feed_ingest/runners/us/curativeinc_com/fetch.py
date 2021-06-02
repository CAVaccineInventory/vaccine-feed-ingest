#!/usr/bin/env python3

import json
import logging
import pathlib
import sys

import urllib3

output_dir = pathlib.Path(sys.argv[1])
base_url = "https://labtools.curativeinc.com/api/v1/testing_sites"

# Configure urllib3
retries = urllib3.util.Retry(status=5, backoff_factor=2, status_forcelist=[500])
http = urllib3.PoolManager(retries=retries)

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("curative")


# Keep track of site_ids that are not found
not_found = set()


def out_of_range(site_id, run=200):
    """
    Handle large gaps in curative data.

    Returns True if the previous {run} sites are not found
    """
    if len(not_found) < run:
        return False

    num_not_found = 0
    for i in range(site_id - 1, site_id - run - 1, -1):
        if i in not_found:
            num_not_found += 1
        else:
            return False

    if num_not_found != run:
        return False

    return True


site_id = 1
while True:
    url = f"{base_url}/{site_id}"
    r = http.request("GET", url)

    obj = json.loads(r.data.decode("utf-8"))
    if "error_code" in obj:
        error_msg = obj.get("error_message", "")
        logger.info(error_msg)
        if error_msg.startswith("No site found"):
            if out_of_range(site_id):
                break
            not_found.add(site_id)
        site_id += 1
        continue

    output_file = output_dir / f"{site_id}.json"
    output_file.write_bytes(r.data)
    site_id += 1
