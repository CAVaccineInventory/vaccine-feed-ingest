#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import sys
import re
from typing import List, Optional

import pydantic
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import provider_id_from_name

logger = getLogger(__file__)

URL_RE = re.compile(r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})")

def _get_address(site: dict) -> schema.Address:
    street1 = site["Street__c"]
    city = site["City__c"]
    zipc = site["Postal_Code__c"]

    if not schema.ZIPCODE_RE.match(zipc):
        zipc = None

    return schema.Address(
            street1=street1,
            street2=None,
            city=city,
            state="IL",
            zip=zipc,
        )

def _get_contact(site: dict) -> List[schema.Contact]:
    contacts = []
    url = site["Website__c"]
    formatted_url = url

    if ' ' in url:
        match = URL_RE.search(url)

        if match and match.group(1):
            formatted_url = match.group(1)

    if not formatted_url.startswith('http'):
        formatted_url = 'http://' + url

    try:
        contacts.append(schema.Contact(website=formatted_url, contact_type=schema.ContactType.BOOKING))
    except pydantic.ValidationError as e:
        logger.warning("Invalid website for id: %s, value: %s, error: %s. Returning empty Contact", site["Id"], url, str(e))

    return contacts

def _get_parent_organization(name: str) -> Optional[schema.Organization]:
    if "Costco" in name:
        return schema.Organization(id=schema.VaccineProvider.COSTCO)
    if "Sam's Pharmacy" in name:
        return schema.Organization(id=schema.VaccineProvider.SAMS)
    if "Walgreen" in name:
        return schema.Organization(id=schema.VaccineProvider.WALGREENS)
    if "Walmart" in name:
        return schema.Organization(id=schema.VaccineProvider.WALMART)
    if "CVS" in name:
        return schema.Organization(id=schema.VaccineProvider.CVS)

    return None

def normalize(site: dict, timestamp: str) -> dict:
    location_id = site["Id"]
    name = site["Testing_Center__c"]
    notes = []

    if "Location_Type__c" in site:
        notes.append(site["Location_Type__c"])

    return schema.NormalizedLocation(
        id=f"sfsites:{location_id}",
        name=name,
        address=_get_address(site),
        location=schema.LatLng(
            latitude=site["Geolocation__Latitude__s"],
            longitude=site["Geolocation__Longitude__s"],
        ),
        contact=_get_contact(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=schema.Availability(appointments=True),
        inventory=None,
        access=None,
        parent_organization=_get_parent_organization(name),
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
