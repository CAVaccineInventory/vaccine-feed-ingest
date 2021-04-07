#!/usr/bin/env python3

import argparse

from ingestors import vaccinespotter

parser = argparse.ArgumentParser(description="Vaccinespotter Fetcher")
parser.add_argument("--state", required=True, help="State to fetch")
parser.add_argument("--geojson-file", required=True, help="Output file")
args = parser.parse_args()

# Fetch raw feeds to raw_output_dir
vaccinespotter.fetch(args.state, args.geojson_file)
