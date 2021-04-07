#!/usr/bin/env python3

import argparse
import json
import logging
import os
import subprocess

import jsonschema

url = "https://www.vaccinespotter.org/api/v0/states/CA.json"

# Configure argparse
parser = argparse.ArgumentParser(description="Vaccinespotter Crawler.")
parser.add_argument("--raw-output-dir", required=True, help="Raw Output Directory")
parser.add_argument("--ndjson-output-file", required=True, help="ndjson output file")
args = parser.parse_args()


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("vaccinateca")

logger.info("Starting Vaccinespotter crawler")


# Fetch
rawfile = os.path.join(args.raw_output_dir, "CA.json")
logger.info(f"Fetching {url} to {rawfile}")
subprocess.run(["conditional-get", url, "-o", rawfile], check=True)


# Parse
with open(rawfile) as fh:
    obj = json.load(fh)

with open(args.ndjson_output_file, "w") as fh:
    for loc in obj["features"]:
        props = loc["properties"]
        long, lat = loc["geometry"]["coordinates"]
        l = jsonschema.Location(
            id=f"vaccinespotter:{props['id']}",  # machinetag not hash
            name=props["name"],
            street1=props["address"],
            city=props["city"],
            state=props["state"],
            zip=props["postal_code"],
            latitude=lat,
            longitude=long,
            website=props["url"],
            provider_id=f"{props['provider']}:{props['provider_location_id']}",
            provider_name=props[
                "provider_brand_name"
            ],  # provider, provider_brand, or provider_brand_name?
        )

        d = jsonschema.to_dict(l)
        json.dump(d, fh)
        fh.write("\n")
