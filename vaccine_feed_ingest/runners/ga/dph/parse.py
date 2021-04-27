#!/usr/bin/env python

import json
import pathlib
import re
import sys
from typing import List

from bs4 import BeautifulSoup
from fetch import location_file_name_for_url


def parse_location(input_file: pathlib.Path) -> dict:
    result: dict = {"phone-numbers": [], "contact-links": []}
    with input_file.open() as f:
        doc = BeautifulSoup(f, "html.parser")
    if doc is None:
        raise Exception("failed to set up beautiful soup")
    main_el = doc.find(id="main-content")
    if main_el is None:
        raise Exception("main content block has changed classes")

    phone_numbers_el = main_el.find(class_="contact-phone-numbers")
    if phone_numbers_el is not None:
        for contact_phone_el in phone_numbers_el.find_all(class_="contact-phone"):
            phone_number = {}
            a = contact_phone_el.find("a")
            if a is not None and a.attrs["href"] is not None:
                phone_number["href"] = a.attrs["href"]
            label = contact_phone_el.find(class_="contact-phone__label")
            if label is not None:
                phone_number["label"] = label.text.strip()
            result["phone-numbers"].append(phone_number)

    for link in main_el.find_all(class_="contact__link"):
        for a in link.find_all("a"):
            result["contact-links"].append(
                {"href": a.attrs["href"], "label": a.text.strip()}
            )

    return result


def parse_landing(input_dir: pathlib.Path) -> List:
    locations_path = input_dir / "locations.html"

    with locations_path.open() as f:
        doc = BeautifulSoup(f, "html.parser")
    if doc is None:
        raise Exception("failed to set up beautiful soup")

    header_cols = doc.select("#datatable > thead > tr > th")
    headers = [h.text.strip() for h in header_cols]
    if not re.search(r"Location Name", headers[0], re.IGNORECASE):
        raise Exception(
            "datatable has changed column header 'Location Name', column order may have changed"
        )
    if not re.search(r"County", headers[1], re.IGNORECASE):
        raise Exception(
            "datatable has changed column header 'County', column order may have changed"
        )
    if not re.search(r"Address", headers[2], re.IGNORECASE):
        raise Exception(
            "datatable has changed column header 'Address', column order may have changed"
        )

    location_rows = doc.select("#datatable > tbody > tr")
    locations = []
    for row in location_rows:
        cells = row.find_all("td")
        location = {
            # these first 3 items are for backwards compatibility with the
            # previous parser iteration
            "Location Name": cells[0].text.strip(),
            "County": cells[1].text.strip(),
            "Address": cells[2].text.strip(),
        }
        a = row.find("a")
        if a is not None and a.attrs["href"] is not None:
            file_name = location_file_name_for_url(a.attrs["href"])
            extras = parse_location(input_dir / file_name)
            location.update(extras)
        locations.append(location)
    return locations


def main():
    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])

    locations = parse_landing(input_dir)

    out_filepath = output_dir / "locations.parsed.ndjson"

    with out_filepath.open("w") as fout:
        for obj in locations:
            json.dump(obj, fout)
            fout.write("\n")


if __name__ == "__main__":
    sys.exit(main())
