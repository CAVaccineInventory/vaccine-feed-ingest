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
    data_id = site["attributes"]["Clinic_ID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis"
    runner = "pa"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "5b874ac0947347e9be49f6847eb44604"
    layer = 0

    return f"{runner}_{site_name}:{arcgis}_{layer}_{data_id}"


def _get_website(site: dict) -> Optional[schema.Contact]:
    if not site["attributes"]["Website"]:
        return None
    website_input = site["attributes"]["Website"].strip()
    # check if the website field contains an email instead, for example
    # "Social media/home page. Please contact Mktg Director first.last@example.com"
    email_match = re.search(r"(?P<email>\S+@\S+)", website_input)
    if email_match:
        logger.info(f"found email in website: '{website_input}'")
        return schema.Contact(email=email_match.group("email"))

    # if the input has any spaces after stripping it, this isn't a url we can parse
    if re.search(r"\s", website_input):
        raise Exception(f"Unable to parse website: '{website_input}'")

    website = re.sub(r"#.*", "", website_input)
    # data observed in the wild may have a malformed scheme
    website = re.sub(r"^(https?)//", r"\1://", website)
    # check if the url scheme is missing
    if re.match(r"^http", website):
        return schema.Contact(website=website)
    website = "http://" + website_input

    return schema.Contact(website=website)


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["Phone_Number"]:
        for phone in normalize_phone(site["attributes"]["Phone_Number"]):
            contacts.append(phone)

    # if site["attributes"]["publicEmail"]:
    #     contacts.append(schema.Contact(email=site["attributes"]["publicEmail"]))

    website_contact = _get_website(site)
    if website_contact:
        contacts.append(website_contact)

    if len(contacts) > 0:
        return contacts

    return None


def _get_zip(site: dict) -> Optional[str]:
    zip = site["attributes"]["ZIP_Code"]
    if not zip:
        return None
    zip = str(zip).strip()
    if re.match(r"\d{9}", zip):
        zip = zip[0:5] + "-" + zip[5:9]
    return zip


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["Facility_Name"],
        address=schema.Address(
            street1=site["attributes"]["Address"],
            street2=site["attributes"]["Address_2"],
            city=site["attributes"]["City"],
            state="PA",
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
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=None,
        source=schema.Source(
            source="pa_arcgis",
            id=site["attributes"]["Clinic_ID"],
            fetched_from_uri="https://padoh.maps.arcgis.com/apps/webappviewer/index.html?id=e6f78224c6fe4313a1f70b56f553c357",  # noqa: E501
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
