#!/usr/bin/env python

import datetime
import json
import pathlib
import pytz
import sys

input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])

input_file = input_dir / "list-providers.json"
output_file = output_dir / "data.parsed.ndjson"


with input_file.open() as fin:
    data = json.load(fin)

eastern = pytz.timezone("US/Eastern")
last_updated = eastern.localize(
    datetime.datetime.strptime(data["lastUpdated"], "%m/%d/%Y, %I:%M:%S %p")
).isoformat()

with output_file.open("w") as fout:
    for site in data["providerList"]:
        site["lastUpdated"] = last_updated
        json.dump(site, fout)
        fout.write("\n")
