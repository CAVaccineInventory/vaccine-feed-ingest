#!/usr/bin/env python

import json
import pathlib
import sys

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

input_file = input_dir / "ct.json"
output_file = output_dir / "data.parsed.ndjson"

with input_file.open() as fin:
    data = json.load(fin)


with output_file.open("w") as fout:
    for site in data:
        json.dump(site, fout)
        fout.write("\n")
