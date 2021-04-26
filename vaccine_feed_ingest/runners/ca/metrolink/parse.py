#!/usr/bin/env python3

import json
import os
import pathlib
import sys

from bs4 import BeautifulSoup

input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])

input_filenames = [p for p in pathlib.Path(input_dir).iterdir() if p.is_file()]

for filename in input_filenames:
    sites = []

    with filename.open() as fin:
        content = fin.read()

    soup = BeautifulSoup(content, "html.parser")
    table = soup.find(id="vaxLocationsTable")
    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")

        site = {
            "site_name": str(cells[0].renderContents())[2:-1].split(" <br/> ")[0],
            "site_address": str(cells[0].renderContents())[2:-1].split(" <br/> ")[1],
            "metrolink_line": cells[1].string,
            "metrolink_station": cells[2].string,
        }
        sites.append(site)

    with (output_dir / (os.path.basename(filename) + ".parsed.ndjson")).open(
        "w"
    ) as fout:
        for site in sites:
            json.dump(site, fout)
            fout.write("\n")
