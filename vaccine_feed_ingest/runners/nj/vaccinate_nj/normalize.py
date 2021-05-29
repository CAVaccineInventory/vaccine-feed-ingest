#!/usr/bin/env python

import pathlib
import sys

import ndjson

output_path = pathlib.Path(sys.argv[1])
input_path = pathlib.Path(sys.argv[2])


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
            "fetched_at": None,  # TODO: not sure how to get this data from parsed data?
            "published_at": vaccination_site_dict["lastCheckedDate"],
            "data": vaccination_site_dict,
        },
    }
    if vaccination_site_dict["phone"] or vaccination_site_dict["url"]:
        normalized_dict["contact"] = []
    if vaccination_site_dict["phone"]:
        normalized_dict["contact"].append({"phone": vaccination_site_dict["phone"]})
    if vaccination_site_dict["url"]:
        normalized_dict["contact"].append({"url": vaccination_site_dict["url"]})
    return normalized_dict


with open(input_path / "data.parsed.ndjson", "r") as f:
    file_contents = ndjson.load(f)
    output_json = [normalize_entry(entry) for entry in file_contents]
with open(output_path / "data.normalized.ndjson", "w") as f:
    ndjson.dump(output_json, f)
