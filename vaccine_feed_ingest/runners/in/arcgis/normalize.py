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


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["GlobalId"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site = "arcgis"
    runner = "in"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "97135bbb1bec488e9717aca061c03e41"
    layer = 0

    return f"{runner}:{site}:{arcgis}_{layer}:{data_id}"


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    vaccines_field = site["attributes"]["Vaccine_Type"].lower().split(",")

    potentials = {
        "Pfizer": schema.Vaccine(vaccine="pfizer"),
        "Moderna": schema.Vaccine(vaccine="moderna"),
        "Johnson & Johnson": schema.Vaccine(vaccine="janssen"),
        "FRPP": schema.Vaccine(vaccine="frpp"),
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

    if site["attributes"]["Site_Location_Info"]:
        contacts.append(schema.Contact(other=site["attributes"]["Site_Location_Info"]))

    if len(contacts) > 0:
        return contacts

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    if site["attributes"]["Site_Special_Inst"]:
        return [site["attributes"]["Site_Special_Inst"]]

    return None


def _get_city(site: dict) -> Optional[str]:
    address = site["attributes"]["Site_Address"]

    return re.search("^.*[.,]\\s(.+), IN (?:[0-9]|-)+$", address).group(0)


def _get_zip(site: dict) -> Optional[str]:
    address = site["attributes"]["Site_Address"]

    return re.search("^.*, IN ((?:[0-9]|-)+)$", address).group(0)


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["Name"],
        address=schema.Address(
            street1=site["attributes"]["Site_Address"],
            street2=None,
            city=_get_city(site),
            state="IN",
            zip=_get_zip(site),
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
            id=site["attributes"]["GlobalID"],
            fetched_from_uri="https://services1.arcgis.com/WzFsmainVTuD5KML/ArcGIS/rest/services/COVID19_Vaccine_Site_Survey_API/FeatureServer/0",  # noqa: E501
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
