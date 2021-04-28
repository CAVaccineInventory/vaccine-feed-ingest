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
logger = logging.getLogger("in/arcgis/normalize.py")


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["GlobalID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis"
    runner = "in"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "46630b2520ce44a68a9f42f8343d3518"
    layer = 0

    return f"{runner}:{site_name}:{arcgis}_{layer}:{data_id}"


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    vaccines = site["attributes"]["Vaccine_Type"]

    inventory = []

    pfizer = re.search("pfizer", vaccines, re.IGNORECASE)
    moderna = re.search("moderna", vaccines, re.IGNORECASE)
    johnson = re.search("janssen", vaccines, re.IGNORECASE) or re.search(
        "johnson", vaccines, re.IGNORECASE
    )

    if pfizer:
        inventory.append(schema.Vaccine(vaccine="pfizer_biontech"))
    if moderna:
        inventory.append(schema.Vaccine(vaccine="moderna"))
    if johnson:
        inventory.append(schema.Vaccine(vaccine="johnson_johnson_janssen"))

    if len(inventory) == 0:
        return None

    return inventory


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["Site_Phone"]:
        sourcePhone = re.sub("[^0-9]", "", site["attributes"]["Site_Phone"])
        if len(sourcePhone) == 11:
            sourcePhone = sourcePhone[1:]
        phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
        contacts.append(schema.Contact(phone=phone))

    if site["attributes"]["Site_Zotec_Link"]:
        contacts.append(schema.Contact(website=site["attributes"]["Site_Zotec_Link"]))
    elif site["attributes"]["Promote_Name"]:
        # Sometimes Promote_Name also contains URLs. These are probably worse
        #   than Site_Zotec_Link, but if they're all that we have we mine as
        #   well use them
        promote_name = site["attributes"]["Promote_Name"]
        # Copied from SO: https://stackoverflow.com/questions/3809401/what-is-a-good-regular-expression-to-match-a-url
        promote_url_match = re.search(
            "https?://(www\\.)?[-a-zA-Z0-9@:%._+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_+.~#?&/=]*)",
            promote_name,
        )
        if promote_url_match:
            contacts.append(schema.Contact(website=promote_url_match.string))

    if site["attributes"]["Site_Location_Info"]:
        contacts.append(schema.Contact(other=site["attributes"]["Site_Location_Info"]))

    if len(contacts) > 0:
        return contacts

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    notes = []
    if site["attributes"]["Site_Special_Inst"]:
        notes.append(site["attributes"]["Site_Special_Inst"])
    if site["attributes"]["Site_Location_Info"]:
        notes.append(site["attributes"]["Site_Location_Info"])

    if len(notes) > 0:
        return notes

    return None


def _get_address(site: dict) -> schema.Address:
    address = site["attributes"]["Site_Address"]
    address_split = address.split(",")

    # TODO: improve address parsing to minimize unknown addresses
    if len(address_split) != 3:
        logger.warning(f"Unparseable address: {address}")
        return None

    return schema.Address(
        street1=address_split[0].strip(),
        city=address_split[-2].strip(),
        state="IN",
        zip=address_split[-1].replace("IN", "").strip(),
    )


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["Name"],
        address=_get_address(site),
        location=schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        ),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=None,
        inventory=_get_inventory(site),
        access=None,
        parent_organization=None,
        links=None,
        notes=_get_notes(site),
        active=None,
        source=schema.Source(
            source="in:arcgis",
            id=site["attributes"]["GlobalID"],
            fetched_from_uri="https://experience.arcgis.com/experience/24159814f1dd4f69b6c22e7e87bca65b",  # noqa: E501
            fetched_at=timestamp,
            published_at=None,
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
