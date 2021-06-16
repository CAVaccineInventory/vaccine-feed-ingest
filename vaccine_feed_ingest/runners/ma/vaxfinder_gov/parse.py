#!/usr/bin/env python

import json
import pathlib
import sys

import csv

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

xlsx_filepath = input_dir / "ma.csv"

locations = []

with open(xlsx_filepath) as csvfile:
    csvreader = csv.DictReader(csvfile, delimiter=' ', quotechar='|')
    for row in csvreader:
        locations.append(row)

out_filepath = output_dir / "data.parsed.ndjson"

with out_filepath.open("w") as fout:
    for location in locations:
        json.dump(location, fout)
        fout.write("\n")
