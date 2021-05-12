#!/usr/bin/env python3

import os
import sys
from urllib.parse import urljoin

import requests

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

output_dir = sys.argv[1]
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

BASE_URL = "https://carbonhealth.com/static/data/"

r = requests.get(urljoin(BASE_URL, "rev-manifest.json"))
r.raise_for_status()
latest = r.json()["covid-vaccine.json"]

r = requests.get(urljoin(BASE_URL, f"rev/{latest}"))
r.raise_for_status()
with open(os.path.join(output_dir, latest), "w") as f:
    f.write(r.text)
