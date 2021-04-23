#!/usr/bin/env python

import json
import pathlib
import sys

import pandas

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

xlsx_filepath = input_dir / "ma.xlsx"

locations = []
data = pandas.read_excel(xlsx_filepath, engine="openpyxl")

for row in data.itertuples():
    locations.append(dict(name=row[1], address=row[3]))

out_filepath = output_dir / "data.parsed.ndjson"

with out_filepath.open("w") as fout:
    for location in locations:
        json.dump(location, fout)
        fout.write("\n")
