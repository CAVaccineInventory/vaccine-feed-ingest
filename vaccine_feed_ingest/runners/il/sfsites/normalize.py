#!/usr/bin/env python

import datetime
import json
import pathlib
import sys

# import schema
site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))
from schema import schema  # noqa: E402

def normalize(site: dict, timestamp: str) -> dict:
    location_id = site["Id"]
    name = site["Testing_Center__c"]

    return schema.NormalizedLocation(
        id=location_id,
        name=name,
        address=schema.Address(
            street1=site["Street__c"],
            street2=None,
            city=site["City__c"],
            state="IL",
            zip=site["Postal_Code__c"],
        ),
        location=schema.LatLng(
            latitude=site["Geolocation__Latitude__s"],
            longitude=site["Geolocation__Longitude__s"],
        ),
        contact=[
            schema.Contact(
                website=site["Website__c"],
                contact_type="booking"
            ),
        ],
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=schema.Availability(appointments=True),
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=None,
        source=schema.Source(
            source="sfsites",
            id=location_id,
            fetched_from_uri="https://coronavirus.illinois.gov/s/vaccination-location",  # noqa: E501
            fetched_at=timestamp,
            published_at=None,
            data=site,
        ),
    )

    return normalized


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

input_filepath = input_dir / "locations.parsed.ndjson"

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

out_filepath = output_dir / "locations.normalized.ndjson"

with input_filepath.open() as fin:
    with out_filepath.open("w") as fout:
        for site_json in fin:
            parsed_site = json.loads(site_json)

            normalized_site = normalize(parsed_site, parsed_at_timestamp)

            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
