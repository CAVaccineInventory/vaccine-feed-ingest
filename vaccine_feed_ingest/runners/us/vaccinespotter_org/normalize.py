#!/usr/bin/env python

import datetime
import json
import pathlib
import sys

from vaccine_feed_ingest.utils.normalize import provider_id_from_name


def normalize(site_blob: dict, timestamp: str) -> dict:
    site = site_blob["properties"]
    geometry = site_blob["geometry"]
    street1 = site["address"]
    street2 = None  # this is part of street1...

    normalized = {
        "id": f"vaccinespotter_org:{site['id']}",
        "name": site["name"],
        "address": {
            "street1": street1,
            "street2": street2,
            "city": site["city"],
            "state": site["state"],
            "zip": site["postal_code"],
        },
        "location": {
            "latitude": geometry["coordinates"][0],
            "longitude": geometry["coordinates"][1],
        },
        "contact": [
            {
                "contact_type": None,
                "phone": None,
                "website": site["url"],
                "other": None,
            },
        ],
        "availability": {
            "appointments": site["appointments_available"],
            "drop_in": None,
        },
        "access": {
            "walk": None,
            "drive": None,
            "wheelchair": None,
        },
        "languages": [],
        "links": [
            {
                "authority": "vaccinespotter_org",
                "id": site["id"],
            },
        ],
        "fetched_at": timestamp,
        "published_at": site[
            "appointments_last_fetched"  # we could also use `appointments_last_modified`
        ],
        "active": None,
        "source": {
            "source": "vaccinespotter_org",
            "id": site["id"],
            "fetched_from_uri": "https://www.vaccinespotter.org/api/v0/US.json",
            "fetched_at": timestamp,
            "published_at": site[
                "appointments_last_fetched"  # we could also use `appointments_last_modified`
            ],
            "data": site_blob,
        },
    }

    parsed_provider_link = provider_id_from_name(
        site["provider_brand_name"]  # or use site["name"]?
    )
    if parsed_provider_link is not None:
        normalized["links"].append(
            {"authority": parsed_provider_link[0], "id": parsed_provider_link[1]}
        )
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)
            normalized_site = normalize(site_blob, parsed_at_timestamp)
            json.dump(normalized_site, fout)
            fout.write("\n")
