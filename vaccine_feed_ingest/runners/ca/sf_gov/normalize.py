#!/usr/bin/env python

import datetime
import json
import pathlib
import sys


def normalize(site: dict, timestamp: str) -> dict:
    address = site["location"]["address"]
    address_parts = [p.strip() for p in address.split(",")]

    # Remove city from end of address
    address_parts.pop()
    street1 = address_parts[0]
    street2 = None
    if len(address_parts) > 1:
        street2 = ", ".join(address_parts[1:])

    return {
        "id": f"sf_gov:{site['id']}",
        "name": site["name"],
        address: {
            "street1": street1,
            "street2": street2,
            "city": site["location"]["city"],
            "state": "CA",
            "zip": site["location"]["zip"],
        },
        "location": {
            "latitude": site["location"]["lat"],
            "longitude": site["location"]["lng"],
        },
        "contact": [
            {
                "contact_type": "booking",
                "phone": site["booking"]["phone"],
                "website": site["booking"]["url"],
                "other": site["booking"]["info"],
            },
        ],
        "availability": {
            "appointments": site["appointments"]["available"],
            "drop_in": site["booking"]["dropins"],
        },
        "access": {
            "walk": site["access_mode"]["walk"],
            "drive": site["access_mode"]["drive"],
            "wheelchair": site["access"]["wheelchair"],
        },
        "languages": [k for k, v in site["access"]["languages"].items() if v],
        "links": [
            {
                "authority": "sf_gov",
                "id": site["id"],
            },
        ],
        "fetched_at": timestamp,
        "published_at": site["appointments"]["last_updated"],
        "active": site["active"],
        "source": {
            "source": "sf_gov",
            "id": site["id"],
            "fetched_from_uri": "https://vaccination-site-microservice.vercel.app/api/v1/appointments",
            "fetched_at": timestamp,
            "published_at": site["appointments"]["last_updated"],
            "data": site,
        },
    }


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.ndjson")

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

for in_filepath in json_filepaths:
    filename = in_filepath.name.split(".", maxsplit=1)[0]
    out_filepath = output_dir / f"{filename}.normalized.ndjson"

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for site_json in fin:
                parsed_site = json.loads(site_json)

                normalized_site = normalize(parsed_site, parsed_at_timestamp)

                json.dump(normalized_site, fout)
                fout.write("\n")
