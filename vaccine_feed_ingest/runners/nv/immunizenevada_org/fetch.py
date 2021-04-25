#!/usr/bin/env python

import json
import pathlib
import sys

import requests

output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "nv.json"

with output_file.open("w") as fout:
    r = requests.post(
        "https://www.immunizenevada.org/views/ajax",
        headers={
            "Referer": "https://www.immunizenevada.org/covid-19-vaccine-locator",
        },
        data={
            "field_type_of_location_value": "All",
            "field_zip_code_value": "",
            "view_name": "vaccine_locator",
            "view_display_id": "block_2",
            "view_path": "/node/2668",
            "_drupal_ajax": 1,
        },
    )

    json.dump(r.json(), fout)
    fout.write("\n")
