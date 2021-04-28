#!/usr/bin/env python

import datetime
import json
import pathlib
import sys

from vaccine_feed_ingest.utils.normalize import organization_from_name

# import schema
site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))
from schema import schema  # noqa: E402


def get_statewide_ids(statewide_filepath: str) -> set:
    statewide_ids = set()

    with statewide_filepath.open() as file:
        for site_json in file:
            parsed_site = json.loads(site_json)

            statewide_ids.add(parsed_site["Id"])

    return statewide_ids


def normalize(site: dict, statewide_ids: set, timestamp: str) -> dict:
    location_id = site["Id"]
    name = site["Testing_Center__c"]
    notes = []

    if "Location_Type__c" in site:
        notes.append(site["Location_Type__c"])

    if location_id not in statewide_ids:
        notes.append("Not open to all Illinois residents")

    parent_organization = None

    org = organization_from_name(name)
    if org is not None:
        parent_organization = schema.Organization(id=org[0], name=org[1])

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
            schema.Contact(website=site["Website__c"], contact_type="booking"),
        ],
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=schema.Availability(appointments=True),
        inventory=None,
        access=None,
        parent_organization=parent_organization,
        links=None,
        notes=notes,
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


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

statewide_ids = get_statewide_ids(input_dir / "statewide.parsed.ndjson")

input_filepath = input_dir / "locations.parsed.ndjson"

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

out_filepath = output_dir / "locations.normalized.ndjson"

with input_filepath.open() as fin:
    with out_filepath.open("w") as fout:
        for site_json in fin:
            parsed_site = json.loads(site_json)

            normalized_site = normalize(parsed_site, statewide_ids, parsed_at_timestamp)

            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
