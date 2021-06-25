#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import normalize_phone, parse_address, normalize_address, normalize_url

logger = getLogger(__file__)

SOURCE_NAME = "tx_harriscounty_gov"

VACCINES_FIELD = {
    "JJ_AVAILABLE": schema.Vaccine(vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN),
    "JJ_AVAILABLE2": schema.Vaccine(vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN),
    "MODERNA_AVAILABLE": schema.Vaccine(vaccine=schema.VaccineType.MODERNA),
    "MODERNA_AVAILABLE2": schema.Vaccine(vaccine=schema.VaccineType.MODERNA),
    "PFIZER_AVAILABLE": schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH),
    "PFIZER_AVAILABLE2": schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH),
}


def _get_availability(site: dict) -> schema.Availability:
    for field in VACCINES_FIELD.keys():
        try:
            if site["attributes"][field] > 0:
                return schema.Availability(appointments=True)
        except KeyError:
            pass

    return None


def _get_id(site: dict) -> str:
    return site["attributes"]["globalid"]


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    inventory = []

    for field, vaccine in VACCINES_FIELD.items():
        try:
            if site["attributes"][field] > 0 and vaccine not in inventory:
                vaccine.supply_level = schema.VaccineSupply.IN_STOCK
                inventory.append(vaccine)
        except KeyError:
            pass

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

    # this field does not seem to exist, but might later
    # see schema at https://services3.arcgis.com/FsUrhUGHe9VfghT8/ArcGIS/rest/services/Weekly_HCPH_VDU_Site_Update_0/FeatureServer/0
    # end_date = site["attributes"].get("close_date")

    if start_date:
        start_date = datetime.datetime.fromtimestamp(start_date / 1000)

    # if end_date:
    #     end_date = datetime.datetime.fromtimestamp(end_date / 1000)

    # if start_date and end_date and start_date > end_date:
    #     return None

    if start_date: # or end_date:
        return [schema.OpenDate(opens=start_date, closes=None)]
    else:
        return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    # Sometimes geometry is not included in the site data
    if site.get("attributes", None):
        location = schema.LatLng(
            latitude=site["attributes"]["lat"],
            longitude=site["attributes"]["lon"],
        )
    else:
        location = None

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site["attributes"]["location"],
        address=_get_address(site),
        location=location,
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=_get_availability(site),
        inventory=_get_inventory(site),
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
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
