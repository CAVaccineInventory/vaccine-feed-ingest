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
from vaccine_feed_ingest.utils.normalize import normalize_phone

logger = getLogger(__file__)


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

    return f"{runner}_{site_name}:{arcgis}_{layer}_{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    if site["attributes"]["USER_Contact_Phone"]:
        for phone in normalize_phone(
            site["attributes"]["USER_Contact_Phone"], contact_type="general"
        ):
            contacts.append(phone)

    if site["attributes"]["USER_Contact_Email"]:
        email = site["attributes"]["USER_Contact_Email"].replace(" ", "")
        if "." not in email:
            return
        if "/" in email:
            split_email = email.split(" / ")
            if len(split_email) == 1:
                split_email = email.split("/")
            if len(split_email) == 1:
                return
            email = split_email[0]
        contacts.append(schema.Contact(contact_type="general", email=email))

    if site["attributes"]["USER_Contact_Website"]:
        contacts.append(
            schema.Contact(
                contact_type="general",
                website=site["attributes"]["USER_Contact_Website"],
            )
        )

    if len(contacts) > 0:
        return contacts

    return None


def _get_address(site: dict) -> schema.Address:
    ZIP_RE = re.compile(r"([0-9]{5})([0-9]{4})")
    zipc = site["attributes"]["USER_Zip_Code"]

    if zipc is not None:
        if ZIP_RE.match(zipc):
            zipc = ZIP_RE.sub(r"\1-\2", zipc)
        length = len(zipc)
        if length != 5 and length != 10:
            zipc = None

    return schema.Address(
        street1=site["attributes"]["USER_Address"],
        street2=site["attributes"]["USER_Address_2"],
        city=site["attributes"]["USER_City"],
        state=site["attributes"]["USER_State"],
        zip=zipc,
    )


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["USER_Provider_Name"],
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
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=None,
        source=schema.Source(
            source="mo_arcgis",
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
