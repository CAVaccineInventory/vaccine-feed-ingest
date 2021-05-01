#!/usr/bin/env python

# this dataset produces duplicates when a single location offers multiple
# vaccine types, or offers both first and second doses. we make no effort to
# deduplicate them here, though lat/long should be sufficient to do so
# downstream.

import datetime
import json
import logging
import pathlib
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import schema  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("tn/vaccinate_gov/normalize.py")

SOURCE_NAME = "tn_vaccinate_gov"


def _get_id(site: dict) -> str:
    return site["id"].replace(" ", "-")


def _get_name(site: dict) -> str:
    # most names are of the form "Descriptive Name - Vaccine available"
    # we will attempt to drop the latter token where it exists
    sep = " - "
    if sep in site["title"]:
        return sep.join(site["title"].replace("\u2013", "-").split(sep)[:-1])
    else:
        return site["title"]


def _get_vaccine_description(site: dict) -> str:
    # most names are of the form "Descriptive Name - Vaccine available"
    # we will attempt to return the last token (or the whole string)
    sep = " - "
    return site["title"].replace("\u2013", "-").split(sep)[-1]


def _get_city(site: dict) -> str:
    return site["address"]["city"]


def _get_address(site: dict) -> Optional[schema.Address]:
    if "address" not in site:
        return None

    return schema.Address(
        street1=site["address"]["lines"][0],
        street2=None
        if len(site["address"]["lines"]) < 2
        else " / ".join(site["address"]["lines"][1:]),
        city=_get_city(site),
        zip=site["address"]["zip"] if site["address"]["zip"] else "0",
        state=site["address"]["state"],
    )


def _get_location(site: dict) -> Optional[schema.LatLng]:
    latitude = site["lat"]
    longitude = site["long"]
    if latitude == "" or longitude == "":
        return None
    return schema.LatLng(
        latitude=float(latitude),
        longitude=float(longitude),
    )


def _get_contacts(site: dict) -> List[schema.Contact]:
    ret = []
    if "phone" in site and site["phone"]:
        raw_phone = str(site["phone"])
        if raw_phone[3] == "-" or raw_phone[7] == "-":
            phone = f"({raw_phone[0:3]}) {raw_phone[4:7]}-{raw_phone[8:12]}"
        elif len(raw_phone) == 10:
            phone = f"({raw_phone[0:3]}) {raw_phone[3:6]}-{raw_phone[6:10]}"
        else:
            phone = raw_phone[0:14]

        ret.append(schema.Contact(phone=phone))

    return ret


def _normalize_date(dt: str) -> str:
    if dt == "":
        return None
    return f"{dt[0:4]}-{dt[4:6]}-{dt[6:8]}"


def _get_inventories(site: dict) -> List[schema.Vaccine]:
    ret = []
    if "Moderna" in _get_vaccine_description(site):
        ret.append(schema.Vaccine(vaccine="moderna"))
    if "Pfizer" in _get_vaccine_description(site):
        ret.append(schema.Vaccine(vaccine="pfizer_biontech"))
    if "J&J" in _get_vaccine_description(site):
        ret.append(schema.Vaccine(vaccine="johnson_johnson_janssen"))
    return ret


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://vaccinate.tn.gov/vaccine-centers/",
        id=_get_id(site),
        source=SOURCE_NAME,
    )


def normalize(site: dict, timestamp: str) -> dict:
    normalized = schema.NormalizedLocation(
        id=(f"{SOURCE_NAME}:{_get_id(site)}"),
        name=_get_name(site),
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        inventory=_get_inventories(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "locations.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "locations.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)

            normalized_site = normalize(site_blob, parsed_at_timestamp)

            json.dump(normalized_site, fout)
            fout.write("\n")
