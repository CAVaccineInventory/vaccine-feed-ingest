#!/usr/bin/env python
import datetime
import os
import pathlib
import sys

import ndjson

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])
timestamp = datetime.datetime.utcnow().isoformat()


def normalize_entry(vaccination_site_dict):
    normalized_dict = {
        "id": "vaccinate_nj:" + vaccination_site_dict["id"],
        "name": vaccination_site_dict["name"],
        "address": {
            "street1": vaccination_site_dict["address"],
            "city": vaccination_site_dict["city"],
            "state": "NJ",
            "zip": vaccination_site_dict["zipCode"],
        },
        "location": {
            "latitude": vaccination_site_dict["geoCode"]["latitude"],
            "longitude": vaccination_site_dict["geoCode"]["longitude"],
        },
        "languages": ["en"],
        "availability": {
            "drop_in": "Walk-in"
            in vaccination_site_dict[
                "scheduling"
            ],  # scheduling is a list of enum strings
            "appointments": vaccination_site_dict["availabilityStatus"] == "Available",
        },
        "active": True,
        "source": {
            "source": "vaccinate_nj",
            "id": vaccination_site_dict["id"],
            "fetched_from_uri": "https://c19vaccinelocatornj.info/api/v1/vaccine/locations/page",
            "fetched_at": timestamp,
            "published_at": vaccination_site_dict["lastCheckedDate"],
            "data": vaccination_site_dict,
        },
    }
    if vaccination_site_dict["phone"] or vaccination_site_dict["url"]:
        normalized_dict["contact"] = []
    if vaccination_site_dict["phone"]:
        normalized_dict["contact"].append({"phone": vaccination_site_dict["phone"]})
    if vaccination_site_dict["url"]:
        if vaccination_site_dict["url"][-1] == "#":
            # HACK: not sure why trailing '#' makes the url invalid.
            url = vaccination_site_dict["url"][:-1]
        else:
            url = vaccination_site_dict["url"]
        normalized_dict["contact"].append({"website": url})
    return normalized_dict


json_filepaths = input_dir.glob("*.ndjson")
for in_filepath in json_filepaths:
    filename, _ = os.path.splitext(in_filepath.name)
    out_filepath = output_dir / f"{filename}.normalized.ndjson"
    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            file_contents = ndjson.load(fin)
            output_json = [normalize_entry(entry) for entry in file_contents]
            ndjson.dump(output_json, fout)
