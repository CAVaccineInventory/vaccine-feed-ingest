#!/usr/bin/env python

import pathlib
import sys

import requests

output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "colorado_gov.html"

with output_file.open("w") as fout:
    r = requests.get(
        "https://covid19.colorado.gov/vaccine/where-you-can-get-vaccinated"
    )

    fout.write(r.text)
    fout.write("\n")
