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


def _get_published_at(site: dict) -> Optional[schema.StringDateTime]:
    time = site["attributes"].get("EditDate")
    if time:
        return datetime.datetime.fromtimestamp(time / 1000)

    return None


def _get_address(site: dict) -> Optional[schema.Address]:
    if site["attributes"]["ADDRESS"] is None:
        return None

    address_field = site["attributes"]["ADDRESS"].replace(",", "").split(" ")
    city_starts = 0
    city_ends = 0

    if site["attributes"]["CITY"] is None:
        # Some sites put all address data in a single field.
        # In this case, he data seems to uppercase all characters in the address.
        # But the city only capitalizes the first letter
        for index, field in enumerate(address_field):
            try:
                if len(field) > 1 and field[1].islower() and city_starts == 0:
                    city_starts = index
                if field == "TX":
                    city_ends = index
                if index == len(address_field) - 1 and city_starts == 0:
                    city_starts = index - 1
                    city_ends = index
            except IndexError as ie:
                logger.error("Unable to parse address: %s", ie)
                return None

        zip = address_field[-1]
        if len(zip) < 5:
            zip = None

        return schema.Address(
            street1=" ".join(address_field[0:city_starts]),
            street2=None,
            city=" ".join(address_field[city_starts:city_ends]),
            state=schema.State.TEXAS,
            zip=zip,
        )

    else:
        # Sometimes the zip can be None, even though the rest of the address has been entered
        if site["attributes"]["ZIP"] is None:
            zip = None
        else:
            zip = str(site["attributes"]["ZIP"])

            # remove typos
            if len(zip) < 5:
                zip = None
            elif re.match(r"\d{6,}", zip):
                zip = None
            elif re.match(r"[a-zA-Z]", zip):
                zip = None

        return schema.Address(
            street1=site["attributes"]["ADDRESS"].strip(),
            street2=None,
            city=site["attributes"]["CITY"].strip(),
            state=schema.State.TEXAS,
            zip=zip,
        )


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    # Sometimes geometry is not included in the site data
    if site.get("geometry", None):
        location = schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        )
    else:
        location = None

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site["attributes"]["NAME"],
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
