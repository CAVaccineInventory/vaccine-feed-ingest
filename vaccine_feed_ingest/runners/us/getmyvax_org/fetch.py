#!/usr/bin/env python3

import pathlib
import sys
from typing import Optional
from urllib.parse import urlencode

import orjson
import requests
import us

from vaccine_feed_ingest.utils.log import getLogger

LOCATIONS_URL = "https://getmyvax.org/api/edge/locations.ndjson"

logger = getLogger(__file__)

output_dir = sys.argv[1]
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

output_path = pathlib.Path(output_dir)

for state in us.STATES_AND_TERRITORIES:
    output_file_path = output_path / f"{state.abbr.lower()}.ndjson"

    query = urlencode({"state": state.abbr, "external_id_format": "v2"})
    next_url: Optional[str] = f"{LOCATIONS_URL}?{query}"

    with output_file_path.open(mode="wb") as out_file:
        while next_url is not None:
            res = requests.get(next_url, stream=True)
            res.raise_for_status()
            next_url = None

            for line in res.iter_lines():
                parsed_line = orjson.loads(line)
                if parsed_line.get("__next__"):
                    next_url = parsed_line["__next__"]
                    break

                out_file.write(line)
                out_file.write(b"\n")
