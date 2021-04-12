#!/usr/bin/env python

import json
import pathlib
import sys

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.json")

for in_filepath in json_filepaths:
    with in_filepath.open() as fin:
        appointment_api_response = json.load(fin)

    filename = in_filepath.name.split(".", maxsplit=1)[0]
    out_filepath = output_dir / f"{filename}.parsed.ndjson"

    with out_filepath.open("w") as fout:
        for site in appointment_api_response["data"]["sites"]:
            json.dump(site, fout)
            fout.write("\n")
