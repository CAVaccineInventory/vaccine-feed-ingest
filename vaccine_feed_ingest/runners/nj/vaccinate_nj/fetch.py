#!/usr/bin/env python

import json
import pathlib
import sys

import requests

LARGE_NUMBER_FAR_EXCEEDING_NUMBER_OF_POTENTIAL_VACCINATION_SITES = 1000000
data = {
    "draw": 1,
    "columns": [],
    "order": [],
    "start": 0,
    "length": LARGE_NUMBER_FAR_EXCEEDING_NUMBER_OF_POTENTIAL_VACCINATION_SITES,
    "search": {"value": "", "regex": False},
}
url = "https://c19vaccinelocatornj.info/api/v1/vaccine/locations/page"
result = requests.post(url, json=data)
assert result.status_code == 200

output_path = pathlib.Path(sys.argv[1])
with open(output_path / "data.json", "w") as f:
    json.dump(json.loads(result.content), f)
