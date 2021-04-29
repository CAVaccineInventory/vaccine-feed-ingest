#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import sys

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.normalize import organization_from_name

logger = logging.getLogger("us/vaccinespotter_org")


def normalize(site: dict, timestamp: str) -> dict:
    location_id = site["Id"]
    name = site["Testing_Center__c"]
    notes = []

    if "Location_Type__c" in site:
        notes.append(site["Location_Type__c"])

    parent_organization = None

    org = organization_from_name(name)
    if org is not None:
        parent_organization = schema.Organization(id=org[0], name=org[1])

    return schema.NormalizedLocation(
        id=f"sfsites:{location_id}",
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
