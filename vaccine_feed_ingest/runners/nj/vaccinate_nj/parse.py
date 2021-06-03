#!/usr/bin/env python

import pathlib
import sys

import ndjson

output_path = pathlib.Path(sys.argv[1])
input_path = pathlib.Path(sys.argv[2])

with open(input_path / "data.json", "r") as f:
    file_contents = ndjson.load(f)
data = file_contents[0]["data"]
with open(output_path / "data.parsed.ndjson", "w") as f:
    ndjson.dump(data, f)
