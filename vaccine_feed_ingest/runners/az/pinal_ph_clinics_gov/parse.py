#!/usr/bin/env python

import json
import pathlib
import sys
from typing import List

from bs4 import BeautifulSoup, PageElement


def find_table(doc: BeautifulSoup) -> PageElement:
    """Locates the table of clinic data"""

    def is_datatable_th(tag):
        return (
            tag.name == "th"
            and tag.has_attr("data-field") is not None
            and tag.text in ["Clinic", "Address", "Nursing Hours"]
        )

    th_el = doc.find(is_datatable_th)
    if th_el is None:
        raise Exception("Couldn't find headers that lead to finding the table of data.")
    table_el = th_el.find_parent("table")
    if table_el is None:
        header_label = th_el.text
        raise Exception(
            f"Couldn't find the table of data. Started at the th element {header_label} and looked for a parent table."
        )
    return table_el


def parse_landing(input_file: pathlib.Path) -> List[dict]:
    """
    Locate the clinic table on page and return a list of dicts where each field
    matches the table header
    """
    with input_file.open() as f:
        doc = BeautifulSoup(f, "html.parser")
    if doc is None:
        raise Exception("failed to set up beautiful soup")

    table_el = find_table(doc)

    headers = [th.text for th in table_el.find("thead").find_all("th")]

    locations = []
    for row in table_el.find("tbody").find_all("tr"):
        location = {}
        for i, tr in enumerate(row.find_all("td")):
            location[headers[i]] = tr.text
        locations.append(location)
    return locations


def main(argv):
    output_dir = pathlib.Path(argv[0])
    input_dir = pathlib.Path(argv[1])

    input_file = input_dir / "office-locations.html"

    locations = parse_landing(input_file)

    out_filepath = output_dir / "locations.parsed.ndjson"
    with out_filepath.open("w") as f:
        for obj in locations:
            json.dump(obj, f)
            f.write("\n")


# If this file is being run from the CLI instead of imported as a module
if __name__ == "__main__":
    # discard the first item in sys.argv as it's the script name.
    # Example: '.../fetch.py'
    argv = sys.argv[1:]

    sys.exit(main(argv))
