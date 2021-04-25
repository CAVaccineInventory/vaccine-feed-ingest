#!/usr/bin/env python

import datetime
import json
import logging
import os
import pathlib
import re
import sys
from typing import List, Optional

# import schema
site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))

from schema import schema  # noqa: E402

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("ak/arcgis/normalize.py")

VACCINES_FIELD = {
    "JJ_AVAILABLE": schema.Vaccine(vaccine="janssen"),
    "JJ_AVAILABLE2": schema.Vaccine(vaccine="janssen"),
    "MODERNA_AVAILABLE": schema.Vaccine(vaccine="moderna"),
    "MODERNA_AVAILABLE2": schema.Vaccine(vaccine="moderna"),
    "PFIZER_AVAILABLE": schema.Vaccine(vaccine="pfizer"),
    "PFIZER_AVAILABLE2": schema.Vaccine(vaccine="pfizer"),
}


def _get_availability(site: dict) -> schema.Availability:
    for field in VACCINES_FIELD.keys():
        try:
            if site["attributes"][field] > 0:
                return schema.Availability(appointments=True)
        except KeyError as e:
            logger.error("Vaccine field not available: %s", e)

    return None


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["OBJECTID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site = "arcgis"
    runner = "tx"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "3078b524189848569f62985d71f4584b"
    layer = 0

    return f"{runner}:{site}:{arcgis}_{layer}:{data_id}"


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    inventory = []

    for field, vaccine in VACCINES_FIELD.items():
        try:
            if site["attributes"][field] > 0 and vaccine not in inventory:
                inventory.append(vaccine)
        except KeyError as e:
            logger.error("Vaccine field not available: %s", e)

    if len(inventory) > 0:
        return inventory

    return None


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["PublicPhone"]:
        sourcePhone = re.sub("[^0-9]", "", site["attributes"]["PublicPhone"])
        if len(sourcePhone) == 11:
            sourcePhone = sourcePhone[1:]
        phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
        contacts.append(schema.Contact(phone=phone))

    if site["attributes"]["WEBSITE"]:
        contacts.append(schema.Contact(website=site["attributes"]["WEBSITE"]))

    if len(contacts) > 0:
        return contacts

    return None


def _get_published_at(site: dict) -> Optional[str]:
    date = site["attributes"]["LAST_UPDATE_VAC"].strip()
    time = site["attributes"]["LAST_UPDATE_TIME_VAC"].strip()
    if date and time:
        date_time_str = date + " " + time
        date_time_obj = datetime.datetime.strptime(date_time_str, "%m/%d/%Y %H:%M:%S")
        return date_time_obj.isoformat()

    return None


def _get_address(site: dict) -> Optional[schema.Address]:
    if site["attributes"]["ADDRESS"] == None:
        return None

    address_field = site["attributes"]["ADDRESS"].replace(",", "").split(" ")
    city_starts = 0
    city_ends = 0

    if site["attributes"]["CITY"] == None:
        # Some sites put all address data in a single field.
        # In this case, he data seems to uppercase all characters in the address.
        # But the city only capitalizes the first letter
        for index, field in enumerate(address_field):
            try:
                if field[1].islower() and city_starts == 0:
                    city_starts = index
                if field == "TX":
                    city_ends = index
            except IndexError as ie:
                logger.error("Unable to parse address: %s", ie)

        return schema.Address(
            street1=" ".join(address_field[0:city_starts]),
            street2=None,
            city=" ".join(address_field[city_starts:city_ends]),
            state="TX",
            zip=address_field[-1],
        )

    else:
        # Sometimes the zip can be None, even though the rest of the address has been entered
        zip = site["attributes"]["ZIP"] if site["attributes"]["ZIP"] != None else 00000

        return schema.Address(
            street1=site["attributes"]["ADDRESS"].strip(),
            street2=None,
            city=site["attributes"]["CITY"].strip(),
            state="TX",
            zip=zip,
        )


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    if site.get("geometry", None):
        location = schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        )
    else:
        location = None

    return schema.NormalizedLocation(
        id=_get_id(site),
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
            source="arcgis",
            id=site["attributes"]["OBJECTID"],
            fetched_from_uri="https://tdem.maps.arcgis.com/apps/webappviewer/index.html?id=3700a84845c5470cb0dc3ddace5c376b",  # noqa: E501
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
