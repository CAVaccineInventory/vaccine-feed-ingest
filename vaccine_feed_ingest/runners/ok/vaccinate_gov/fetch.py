#!/usr/bin/env python3

import json
import os
import sys

import requests
from bs4 import BeautifulSoup

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

csrf_url = "https://vaccinate.oklahoma.gov/_layout/tokenhtml"
api_url = "https://vaccinate.oklahoma.gov/EntityList/Map/Search/"

output_dir = sys.argv[1]
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

session = requests.Session()

response = session.get(csrf_url)
soup = BeautifulSoup(response.text, "html.parser")

form_csrf = soup.find_all("input")[0].get("value")

headers = {
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "__RequestVerificationToken": form_csrf,
}
payload = {
    "longitude": -97.5212631225586,
    "latitude": 35.4684944152832,
    "distance": 400,
    "units": "miles",
    "id": "d0f0b903-e994-ea11-a811-000d3a33f3c3",
}

output = session.post(api_url, data=json.dumps(payload), headers=headers)

file = open(os.path.join(output_dir, "output.json"), "w")
file.write(output.text)
file.close()
