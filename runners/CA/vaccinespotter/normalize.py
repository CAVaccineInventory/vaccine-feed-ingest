#!/usr/bin/env python3

import argparse
from ingestors import vaccinespotter

parser = argparse.ArgumentParser(description="Vaccinespotter Normalizer")
parser.add_argument("--ndjson-file", required=True, help="ndjson input file")
parser.add_argument(
    "--normalized-ndjson-file", required=True, help="ndjson output file"
)
args = parser.parse_args()

# Parse geojson feeds to ndjson feeds
vaccinespotter.normalize(args.ndjson_file, args.normalized_ndjson_file)
