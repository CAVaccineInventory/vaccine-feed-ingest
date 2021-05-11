#!/usr/bin/env python3

import os
import sys

import requests

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

output_dir = sys.argv[1]
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

base_url = "https://carbonhealth.com/static/data"

latest = requests.get(f"{base_url}/rev-manifest.json").json()["covid-vaccine.json"]

r = requests.get(f"{base_url}/rev/{latest}")
with open(os.path.join(output_dir, "output.json"), "w") as f:
    f.write(r.text)
