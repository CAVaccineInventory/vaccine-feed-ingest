#!/usr/bin/env python3

import json
import pathlib
import re
import sys

from bs4 import BeautifulSoup

input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.parsed.ndjson"

input_filenames = [p for p in pathlib.Path(input_dir).iterdir() if p.is_file()]


def find_data_item(parent, label, offset):
    try:
        row_matches = [
            x for x in parent.find_all(["p", "div"]) if label in x.get_text()
        ]
        content = row_matches[-1].contents[offset]
        return (
            content.strip() if isinstance(content, str) else content.get_text().strip()
        )
    except Exception:
        return ""


EXTRACT_CLINIC_ID = re.compile(r".*clinic(\d*)\.png")


with output_file.open("w") as fout:
    for filename in input_filenames:
        text = open(filename, "r").read()
        soup = BeautifulSoup(text, "html.parser")

        # classes only used on titles for search results
        for title in soup.select(".text-xl.font-black"):
            parent = title.parent
            combined_name = title.get_text().strip()
            name, date = combined_name.split(" on ")
            address = title.find_next_sibling("p").get_text().strip()
            vaccines = find_data_item(parent, "Vaccinations offered", -2)
            ages = find_data_item(parent, "Age groups served", -1)
            additional_info = find_data_item(parent, "Additional Information", -1)
            hours = find_data_item(parent, "Clinic Hours", -1)
            available_count = find_data_item(parent, "Available Appointments", -1) or 0
            special = find_data_item(parent, "Special Instructions", -1)

            find_clinic_id = EXTRACT_CLINIC_ID.match(
                parent.find_next_sibling("div", "map-image").find("img")["src"]
            )
            clinic_id = find_clinic_id.group(1)
            data = {
                "name": name,
                "date": date,
                "address": address,
                "vaccines": vaccines,
                "ages": ages,
                "info": additional_info,
                "hours": hours,
                "available": available_count,
                "special": special,
                "clinic_id": clinic_id,
            }
            json.dump(data, fout)
            fout.write("\n")
