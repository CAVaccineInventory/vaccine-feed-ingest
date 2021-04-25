#!/usr/bin/env python

import pathlib
import sys

import requests

output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "maine_gov.html"

with output_file.open("w") as fout:
    r = requests.get("https://www.maine.gov/covid19/vaccines/vaccination-sites")

    fout.write(r.text)
    fout.write("\n")
