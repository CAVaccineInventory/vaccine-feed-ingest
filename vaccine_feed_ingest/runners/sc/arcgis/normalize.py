#!/usr/bin/env python

import datetime
import json
import logging
import os
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("sc/arcgis/normalize.py")

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.ndjson")

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["GlobalID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis"
    runner = "sc"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "bbd8924909264baaa1a5a1564b393063"
    layer = 0

    return f"{runner}_{site_name}:{arcgis}_{layer}:{data_id}"


# This currently tosses any address if it doesn't have a street address or zip because
# the schema doesn't allow optionals for those
def _get_address(site: dict) -> Optional[schema.Address]:
    if (
        parsed_site["attributes"]["SiteAddress"] is None
        or parsed_site["attributes"]["SiteZip"] is None
    ):
        return None

    return schema.Address(
        street1=site["attributes"]["SiteAddress"],
        street2=site["attributes"]["SiteAddressDetail"],
        city=site["attributes"]["SiteCity"],
        state="SC",
        zip=site["attributes"]["SiteZip"],
    )


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["SitePhone"]:
        sourcePhone = site["attributes"]["SitePhone"].lower()
        # Some numbers in the data have extensions (e.g. 1-855-222-0083 ext 513) which we
        # are currently not capturing because schema doesn't seem to have space for it
        if "ext" in sourcePhone:
            sourcePhone = sourcePhone.split("ext")[0]
        sourcePhone = re.sub("[^0-9]", "", sourcePhone)
        if len(sourcePhone) == 11:
            sourcePhone = sourcePhone[1:]
        phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
        contacts.append(schema.Contact(phone=phone))

    # Contacts seems to be a free text field where people usually enter emails but also sometimes
    # other stuff like numbers, hours of operation, etc
    if site["attributes"]["Contact"]:
        if "@" in site["attributes"]["Contact"]:
            contacts.append(schema.Contact(email=site["attributes"]["Contact"]))
        else:
            contacts.append(schema.Contact(other=site["attributes"]["Contact"]))

    if site["attributes"]["URL"]:
        contacts.append(schema.Contact(website=site["attributes"]["URL"]))

    if len(contacts) > 0:
        return contacts

    return None


# Using "Appointments" field though unclear whether this should be interpreted as
# "An appointment is required" or "An appointment is available"
def _get_activated(site: dict) -> bool:
    if site["attributes"]["Activated1"] == "No":
        return False
    else:
        return True


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    if site["attributes"]["V_Manufacturer"]:
        vaccines_field = map(
            lambda str: str.strip(),
            site["attributes"]["V_Manufacturer"].lower().split(","),
        )

        potentials = {
            "pzr": schema.Vaccine(vaccine="pfizer"),
            "pfr": schema.Vaccine(vaccine="pfizer"),
            "pfizer": schema.Vaccine(vaccine="pfizer"),
            "mod": schema.Vaccine(vaccine="moderna"),
            "moderna": schema.Vaccine(vaccine="moderna"),
            "jj": schema.Vaccine(vaccine="janssen"),
            "jjj": schema.Vaccine(vaccine="janssen"),
        }

        inventory = []

        for vf in vaccines_field:
            try:
                inventory.append(potentials[vf])
            except KeyError as e:
                logger.error("Unexpected vaccine type: %s", e)

        if len(inventory) > 0:
            return inventory

    return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["loc_name"],
        address=_get_address(site),
        location=schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        ),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        # There is an "Appointments" field in the data though it is unclear whether this should be interpreted as
        # "An appointment is required" or "An appointment is available". Leaving blank as this information
        # will likely need phone bankers and/or web team to find availability
        availability=None,
        inventory=_get_inventory(site),
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=_get_activated(site),
        source=schema.Source(
            source="sc_arcgis",
            id=site["attributes"]["GlobalID"],
            fetched_from_uri="https://opendata.arcgis.com/datasets/bbd8924909264baaa1a5a1564b393063_0.geojson",  # noqa: E501
            fetched_at=timestamp,
            data=site,
        ),
    )


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
