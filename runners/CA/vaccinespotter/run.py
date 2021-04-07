#!/usr/bin/env python3

import argparse
import logging
import subprocess

from os.path import join, dirname


state = "CA"
url = "https://www.vaccinespotter.org/api/v0/states/CA.json"

# Configure argparse
parser = argparse.ArgumentParser(description="Vaccinespotter Runner")
parser.add_argument("--output-dir", required=True, help="Output Directory")
args = parser.parse_args()


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("run")

logger.info(f"Starting Vaccine Spotter {state} crawler")

# Fetch
cmd = join(dirname(__file__), "fetch.py")
geojson_file = join(args.output_dir, f"{state}.geojson")
subprocess.run([cmd, "--state", state, "--geojson-file", geojson_file], check=True)


# Parse -- separate location records into ndjson
cmd = join(dirname(__file__), "parse.py")
ndjson_file = join(args.output_dir, f"{state}.ndjson")
subprocess.run(
    [cmd, "--geojson-file", geojson_file, "--ndjson-file", ndjson_file], check=True
)


# Normalize -- transform ndjson to formatted ndjson
cmd = join(dirname(__file__), "normalize.py")
normalized_ndjson_file = join(args.output_dir, f"{state}.normalized.ndjson")
subprocess.run(
    [
        cmd,
        "--ndjson-file",
        ndjson_file,
        "--normalized-ndjson-file",
        normalized_ndjson_file,
    ],
    check=True,
)
