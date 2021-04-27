#!/usr/bin/env python

import datetime
import json
import logging
import os
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import schema

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("ri/arcgis/normalize.py")


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["OBJECTID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis"
    runner = "ri"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "da57e8c8663048a2a9893c636fef63d0"
    layer = 0

    return f"{runner}:{site_name}:{arcgis}_{layer}:{data_id}"


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    vaccines_field = site["attributes"]["USER_VaxType"].lower().split(",")

    potentials = {
        "pfizer-biontech covid-19 vaccine": schema.Vaccine(vaccine="pfizer_biontech"),
        "moderna covid-19 vaccine": schema.Vaccine(vaccine="moderna"),
        "janssen": schema.Vaccine(vaccine="johnson_johnson_janssen"),
        "jjj": schema.Vaccine(vaccine="johnson_johnson_janssen"),
        "janssen (johnson & johnson) covid-19 vaccine": schema.Vaccine(
            vaccine="johnson_johnson_janssen"
        ),
    }

    inventory = []

    for vf in vaccines_field:
        try:
            if vf != "-" and vf != "\x08":  # "-" is listed when no data given
                inventory.append(potentials[vf])
        except KeyError as e:
            logger.error("Unexpected vaccine type: %s", e)

    if len(inventory) > 0:
        return inventory

    return None


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["USER_Scheduling_by_Phone"]:
        sourcePhone = re.sub(
            "[^0-9]", "", site["attributes"]["USER_Scheduling_by_Phone"]
        )
        if len(sourcePhone) == 10:
            phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
            contacts.append(schema.Contact(phone=phone))

    if site["attributes"]["USER_Link_to_Sign_Up"]:
        contacts.append(
            schema.Contact(website=site["attributes"]["USER_Link_to_Sign_Up"])
        )

    if len(contacts) > 0:
        return contacts

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    if site["attributes"]["USER_Eligibility"]:
        return [site["attributes"]["USER_Eligibility"]]

    return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["USER_Name"],
        address=schema.Address(
            street1=site["attributes"]["USER_Address"],
            street2=None,
            city=site["attributes"]["USER_City_Town"],
            state="RI",
            zip=site["attributes"]["ZIPCODE"],
        ),
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
            source="arcgis",
            id=site["attributes"]["OBJECTID"],
            fetched_from_uri="https://rihealth.maps.arcgis.com/apps/instant/nearby/index.html?appid=a25f35833533498bac3f724f92a84b4e",  # noqa: E501
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
