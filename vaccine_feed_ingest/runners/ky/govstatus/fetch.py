#!/usr/bin/env python

import pathlib
import sys

import requests

output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "govstatus.html"

with output_file.open("w") as fout:
    r = requests.get("https://govstatus.egov.com/kentucky-vaccine-map")

    fout.write(r.text)
    fout.write("\n")
