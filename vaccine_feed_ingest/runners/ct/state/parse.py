#!/usr/bin/env python

import json
import pathlib
import sys

input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.parsed.ndjson"

results = []
for input_file in input_dir.glob("data.raw.*.json"):
    with input_file.open() as fin:
        results.extend(json.load(fin)["results"])

with output_file.open("w") as fout:
    for result in results:
        json.dump(result, fout)
        fout.write("\n")
