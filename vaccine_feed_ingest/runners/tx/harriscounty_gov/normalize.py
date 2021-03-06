#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional

from opening_hours import OpeningHours
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import (
    normalize_address,
    normalize_phone,
    normalize_url,
    parse_address,
)
from vaccine_feed_ingest.utils.parse import location_id_from_name

logger = getLogger(__file__)

SOURCE_NAME = "tx_harriscounty_gov"

VACCINES_FIELD = {
    "Moderna": schema.Vaccine(vaccine=schema.VaccineType.MODERNA),
    "Pfizer": schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH),
    "JohnsonJohnson": schema.Vaccine(
        vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN
    ),
}


def _get_id(site: dict) -> str:
    return site["attributes"]["globalid"]


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    inventory = []

    if vaccine := site["attributes"].get("typeofvaccine"):
        for vax_type, vax in VACCINES_FIELD.items():
            if vax_type in vaccine:
                inventory.append(vax)

        if len(inventory) != len(vaccine.split(",")):
            logger.warn(
                "some vaccine types were not automatically detected in string: "
                + vaccine
            )

    if len(inventory) > 0:
        return inventory

    return None


def _website_fixup(website):
    if website is None:
        return None

    website = website.strip()

    if website in ["no", "N/A"]:
        return None

    if re.search(r"@\S+.(com|org|gov)$", website):
        # Actually an email
        return None

    if website.endswith("#"):
        website = website[:-1]

    if not website.startswith("http"):
        website = "http://" + website

    return website


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    phones = normalize_phone(site["attributes"]["phone"])

    contacts.extend(phones)

    website = normalize_url(site["attributes"]["website"])
    if website is not None:
        contacts.append(
            schema.Contact(website=website, contact_type=schema.ContactType.GENERAL)
        )

    if len(contacts) > 0:
        return contacts

    return None


def _get_published_at(site: dict) -> Optional[str]:
    time = site["attributes"].get("EditDate")
    if time:
        return datetime.datetime.fromtimestamp(time / 1000).isoformat()

    return None


def _get_address(site: dict) -> Optional[schema.Address]:

    addr = parse_address(site["attributes"]["address"])

    addr = normalize_address(addr)

    return addr


def _get_opening_dates(site: dict) -> Optional[List[schema.OpenDate]]:

    start_date = site["attributes"].get("opendate")

    if start_date:
        start_date = datetime.datetime.fromtimestamp(start_date / 1000)

    if start_date:
        return [schema.OpenDate(opens=start_date, closes=None)]
    else:
        return None


def _get_notes(site: dict) -> Optional[List[str]]:
    notelist = []
    if notes := site["attributes"].get("notes"):
        notelist.append(notes)

    if oh_notes := site["attributes"].get("opening_hours_notes"):
        notelist.append(oh_notes)

    if len(notelist) > 0:
        return notelist

    return None


def _get_location(site: dict) -> Optional[schema.LatLng]:
    # Sometimes geometry is not included in the site data
    lat = site["attributes"].get("lat")
    lon = site["attributes"].get("lon")
    if lat and lon:
        return schema.LatLng(
            latitude=lat,
            longitude=lon,
        )
    elif site.get("geometry", None):
        return schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        )
    else:
        return None


def _get_parent(site: dict) -> Optional[schema.Organization]:
    if org := site["attributes"].get("agency"):
        return schema.Organization(id=location_id_from_name(org), name=org)
    return None


def _get_opening_hours(site):
    oh = site["attributes"].get("operatinghours")
    if oh:
        try:
            return OpeningHours.parse(oh).json()
        except Exception:
            # store the notes back in the dict so the notes function can grab it later
            site["opening_hours_notes"] = "Hours: " + oh
    else:
        return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site["attributes"]["location"],
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=_get_opening_dates(site),
        opening_hours=_get_opening_hours(site),
        availability=None,
        inventory=_get_inventory(site),
        access=None,
        parent_organization=_get_parent(site),
        links=None,
        notes=_get_notes(site),
        active=None,
        source=schema.Source(
            source=SOURCE_NAME,
            id=_get_id(site),
            fetched_from_uri="https://publichealth.harriscountytx.gov/Resources/2019-Novel-Coronavirus/Register-for-COVID-19-Vaccine",  # noqa: E501
            fetched_at=timestamp,
            published_at=_get_published_at(site),
            data=site,
        ),
    )


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.ndjson")

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

for in_filepath in json_filepaths:
    filename, _ = os.path.splitext(in_filepath.name)
    out_filepath = output_dir / f"{filename}.normalized.ndjson"

    logger.info(
        "normalizing %s => %s",
        in_filepath,
        out_filepath,
    )

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for site_json in fin:
                parsed_site = json.loads(site_json)

                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
