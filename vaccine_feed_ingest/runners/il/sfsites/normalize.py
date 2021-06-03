#!/usr/bin/env python

import datetime
import json
import pathlib
import re
import sys
from typing import List, Optional, Set

import pydantic
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

URL_RE = re.compile(
    r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"
)


def _id(loc: schema.LatLng, name: str, addr: schema.Address) -> str:
    # The ids provided in this site's data are not stable, so we need to
    # construct our own stable id.
    #
    # Here we use the lat/lng to a precision of 4 decimal places, the name, and
    # the first line of the address (without the first line of the address,
    # there are several id collisions for separate locations - perhaps where
    # the data source has the improper lat/lng for one).
    #
    # This technique means the id will be unstable when these fields are changed
    # in the data source, but that's a better outcome than being unstable on
    # each run. Additionally, we truncate the name and address to attempt to be
    # a bit more stable.
    return re.sub(
        "[^a-zA-Z0-9-_]",
        "_",
        f"{loc.latitude:.4f}_{loc.longitude:.4f}_{name[:16].upper()}_{addr.street1[:16].upper()}",
    )


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
        state=schema.State.ILLINOIS,
        zip=zipc,
    )


def _get_contact(site: dict) -> List[schema.Contact]:
    contacts = []
    url = site["Website__c"]
    formatted_url = url

    if " " in url:
        match = URL_RE.search(url)

        if match and match.group(1):
            formatted_url = match.group(1)

    if not formatted_url.startswith("http"):
        formatted_url = "http://" + url

    try:
        contacts.append(
            schema.Contact(
                website=formatted_url, contact_type=schema.ContactType.BOOKING
            )
        )
    except pydantic.ValidationError:
        logger.warning(
            "Invalid website for id: %s, value: %s. Returning empty Contact",
            site["Id"],
            url,
        )

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


def normalize(site: dict, timestamp: str) -> schema.NormalizedLocation:
    source_id = "il_sfsites"
    name = site["Testing_Center__c"]
    notes = []

    loc = schema.LatLng(
        latitude=site["Geolocation__Latitude__s"],
        longitude=site["Geolocation__Longitude__s"],
    )
    addr = _get_address(site)

    location_id = _id(loc, name, addr)

    if "Location_Type__c" in site:
        notes.append(site["Location_Type__c"])

    return schema.NormalizedLocation(
        id=f"{source_id}:{location_id}",
        name=name,
        address=addr,
        location=loc,
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
            source=source_id,
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

ids_seen: Set[str] = set()

with input_filepath.open() as fin:
    with out_filepath.open("w") as fout:
        for site_json in fin:
            parsed_site = json.loads(site_json)

            normalized_site = normalize(parsed_site, parsed_at_timestamp)

            # This data source contains some duplicates with slightly different
            # formatting (e.g., the address is ALL CAPS on one copy). In the
            # case where this duplication causes us to repeat an id, drop all
            # records other than the first occurrence. Note that some of the
            # modifications we make in order to make ids more stable also
            # increase the risk of id re-use with this type of duplicate.
            #
            # This still passes through several duplicates where the formatting
            # is meaningfully different (e.g., North vs. N.).
            if normalized_site.id in ids_seen:
                logger.warning(
                    "id %s is being reused. Dropping the reused location: %s",
                    normalized_site.id,
                    normalized_site,
                )
                continue

            ids_seen.add(normalized_site.id)

            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
