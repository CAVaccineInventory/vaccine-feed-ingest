#!/usr/bin/env python3

import argparse
import sys
from ingestors import vaccinespotter

parser = argparse.ArgumentParser(description="Vaccinespotter Parser")
parser.add_argument("--geojson-file", required=True, help="geojson input file")
parser.add_argument("--ndjson-file", required=True, help="ndjson output file")
args = parser.parse_args()

# Parse geojson feeds to ndjson feeds
vaccinespotter.parse(args.geojson_file, args.ndjson_file)
