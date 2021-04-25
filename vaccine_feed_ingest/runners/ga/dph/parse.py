#!/usr/bin/env python

import json
import pathlib
import sys

from bs4 import BeautifulSoup

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

locations_path = input_dir / "locations.html"

with locations_path.open() as fin:
    doc = BeautifulSoup(fin, "html.parser")

header_cols = doc.select("#datatable > thead > tr > th")
location_rows = doc.select("#datatable > tbody > tr")

out_filepath = output_dir / "locations.parsed.ndjson"

with out_filepath.open("w") as fout:
    for row in location_rows:
        obj = {}
        for i, col in enumerate(row.find_all("td"), start=0):
            obj[header_cols[i].text] = col.text.strip()
        json.dump(obj, fout)
        fout.write("\n")
