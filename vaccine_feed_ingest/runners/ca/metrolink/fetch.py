#!/usr/bin/env python

import pathlib
import sys

import requests

output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "metrolink.html"

with output_file.open("w") as fout:
    r = requests.get(
        "https://register.metrolinktrains.com/web-assets/vax-sites/index.php"
    )

    fout.write(r.text)
    fout.write("\n")
