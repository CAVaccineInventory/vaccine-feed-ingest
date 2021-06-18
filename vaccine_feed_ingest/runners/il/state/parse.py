#!/usr/bin/env python

import csv
import json
import pathlib
import sys

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

csv_filepath = input_dir / "il_state.csv"

locations = []


def transpose_keys(input_dict):
    keymap = {
        "Site Name": "name",
        "Location Type": "loc_type",
        "Address": "address",
        "City": "city",
        "Zip": "zip",
        "County": "county",
        "pindetails": "pindetails",
        "LATITUDE": "lat",
        "LONGITUDE": "long",
        "Website": "website",
    }

    newdict = {}
    for key, value in input_dict.items():
        newdict[keymap[key]] = value

    return newdict


with open(csv_filepath) as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        locations.append(transpose_keys(row))

out_filepath = output_dir / "data.parsed.ndjson"

with out_filepath.open("w") as fout:
    for location in locations:
        json.dump(location, fout)
        fout.write("\n")
