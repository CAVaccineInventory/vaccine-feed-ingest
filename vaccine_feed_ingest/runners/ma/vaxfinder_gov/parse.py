#!/usr/bin/env python

import csv
import json
import pathlib
import sys

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

xlsx_filepath = input_dir / "ma.csv"

locations = []


def transpose_keys(input_dict):
    keymap = {
        'Location Name': "name",
        'Serves': 'serves',
        'Full Address': 'address',
        'Site Type': 'site_type',
        'Find an appointment': 'appointment_link',
        'Phone': 'phone',
        'E-mail': 'email',
        'Appointment info': 'additional_info',
        'Instructions at site': 'instructions',
        'Accessibility': 'accessibility',
        'Days of the week open': 'days_open',
        'Vaccines available': 'vaccines_available',
        'Website': 'website'
    }

    newdict = {}
    for key, value in input_dict.items():
        newdict[keymap[key]] = value

    return newdict


with open(xlsx_filepath) as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        locations.append(transpose_keys(row))

out_filepath = output_dir / "data.parsed.ndjson"

with out_filepath.open("w") as fout:
    for location in locations:
        json.dump(location, fout)
        fout.write("\n")
