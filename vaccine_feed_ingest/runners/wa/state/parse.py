#!/usr/bin/env python

import json
import pathlib
import sys

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


if sys.argv[1] is None:
    logger.error("Must pass an output_dir as second argument")
    sys.exit(1)

if sys.argv[2] is None:
    logger.error("Must pass an input_dir as second argument")
    sys.exit(1)

input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.parsed.ndjson"

results = []
for input_file in input_dir.glob("*.json"):
    with input_file.open() as fin:
        data = json.load(fin)

        if not data:
            logger.error("No data!")
            continue

        if not data.get("data"):
            logger.error("No data data!")
            continue

        if not data["data"].get("searchLocations"):
            logger.error("No data data searchLocations!")
            continue

        if not data["data"]["searchLocations"]["locations"]:
            logger.warning("No data data searchLocations locations!")
            continue

        locations = data["data"]["searchLocations"]["locations"]
        results.extend(locations)

with output_file.open("w") as fout:
    for result in results:
        json.dump(result, fout)
        fout.write("\n")
