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
logger = logging.getLogger("mo/arcgis/normalize.py")


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["GlobalID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis"
    runner = "mo"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "46630b2520ce44a68a9f42f8343d3518"
    layer = 0

    return f"{runner}:{site_name}:{arcgis}_{layer}:{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["USER_Contact_Phone"]:
        sourcePhone = re.sub("[^0-9]", "", site["attributes"]["USER_Contact_Phone"])
        if len(sourcePhone) == 11:
            sourcePhone = sourcePhone[1:]
        phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
        contacts.append(schema.Contact(phone=phone))

    if site["attributes"]["USER_Contact_Email"]:
        contacts.append(schema.Contact(email=site["attributes"]["USER_Contact_Email"]))

    if site["attributes"]["USER_Contact_Website"]:
        contacts.append(
            schema.Contact(website=site["attributes"]["USER_Contact_Website"])
        )

    if len(contacts) > 0:
        return contacts

    return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["USER_Provider_Name"],
        address=schema.Address(
            street1=site["attributes"]["USER_Address"],
            street2=site["attributes"]["USER_Address_2"],
            city=site["attributes"]["USER_City"],
            state=site["attributes"]["USER_State"],
            zip=site["attributes"]["USER_Zip_Code"],
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
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=None,
        source=schema.Source(
            source="arcgis",
            id=site["attributes"]["GlobalID"],
            fetched_from_uri="https://www.arcgis.com/apps/webappviewer/index.html?id=ab04156a03584e31a14ae2eb36110c20",  # noqa: E501
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
