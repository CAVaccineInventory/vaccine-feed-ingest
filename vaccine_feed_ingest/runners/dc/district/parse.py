#!/usr/bin/env python3

import json
import pathlib
import sys

from bs4 import BeautifulSoup

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.html"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.parsed.ndjson"

with input_file.open() as fin:
    soup = BeautifulSoup(fin, "html.parser")

table = soup.find("h4", string="Walk-Up Locations").find_next_sibling("table")
headers = [h.text.strip().replace("\xa0", " ") for h in table.select("thead th")]

sites = []
for row in table.select("tbody tr"):
    cells = [c.text.strip().replace("\xa0", " ") for c in row.find_all("td")]
    sites.append(dict(zip(headers, cells)))

with output_file.open("w") as fout:
    for site in sites:
        json.dump(site, fout)
        fout.write("\n")
