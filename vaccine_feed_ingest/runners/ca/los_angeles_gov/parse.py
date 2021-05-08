#!/usr/bin/env python

import json
import pathlib
import sys

input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])

input_file = input_dir / "los_angeles.js"
output_file = output_dir / "los_angeles.parsed.ndjson"

with input_file.open() as fin:
    content = fin.read()

    unfiltered = json.loads(content.lstrip("var unfiltered = "))

    with open(output_file, "w") as fout:
        for location in unfiltered:
            json.dump(location, fout)
            fout.write("\n")
