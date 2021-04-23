#!/usr/bin/env python

import csv
import json
import pathlib
import sys

import pandas

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

csv_filepaths = input_dir.glob("*.xlsx")

for in_filepath in csv_filepaths:
    locations = []
    data = pandas.read_excel(in_filepath, engine="openpyxl")

    for row in data.itertuples():
        locations.append(dict(name=row[1], address=row[3]))

    out_filepath = output_dir + "/data.parsed.ndjson"

    with out_filepath.open("w") as fout:
        for location in locations:
            json.dump(location, fout)
            fout.write("\n")
