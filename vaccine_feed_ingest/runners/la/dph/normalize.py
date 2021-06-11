#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import sys
from typing import List, Optional, OrderedDict

import us
import usaddress
from opening_hours import OpeningHours
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import (
    normalize_address,
    normalize_phone,
    normalize_zip,
    parse_address,
)

logger = getLogger(__file__)


SOURCE_NAME = "la_dph"


class CustomBailError(Exception):
    pass


def _get_id(site: dict) -> str:
    return site.get("id", "")


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if phone := site.get("phone_number"):
        contacts.extend(normalize_phone(phone))

    if len(contacts) > 0:
        return contacts

    return None


def sanitize_url(url):
    url = url.strip()
    url = url.replace("#", "")
    url = url.replace("\\", "/")  # thanks windows
    url = url if url.startswith("http") else "https://" + url
    if len(url.split(" ")) == 1:
        return url
    return None


def _get_notes(site: dict) -> Optional[List[str]]:

    notes = []
    if directions := site.get("directions_link"):
        notes.append("Get directions: " + directions)

    if notes := site.get("notes"):
        notes.append(notes)

    if notes != []:
        return notes

    return None


def _get_access(site: dict) -> Optional[List[str]]:

    # "transport": {"walk": true, "drive": null}
    transport = site.get("transport", {"walk": None, "drive": None})

    return schema.Access(drive=transport.get("drive"), walk=transport.get("walk"))


def try_lookup(mapping, value, default, name=None):
    if value is None:
        return default

    try:
        return mapping[value]
    except KeyError as e:
        name = " for " + name or ""
        logger.warn("value not present in lookup table%s: %s", name, e)

        return default


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:

    vaccines = []
    vax_default = {"pfizer": None, "moderna": None, "janssen": None}
    vax1 = site.get("vaccines_dose_1") or vax_default
    vax2 = site.get("vaccines_dose_2") or vax_default

    pfizer = vax1.get("pfizer", False) or vax2.get("pfizer", False)
    moderna = vax1.get("moderna", False) or vax2.get("moderna", False)
    janssen = vax1.get("janssen", False) or vax2.get("janssen", False)

    if pfizer:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH))
    if moderna:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.MODERNA))
    if janssen:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN))

    return vaccines


# def try_get_list(lis, index, default=None):
#     if lis is None:
#         return default

#     try:
#         value = lis[index]
#         if value == "none":
#             logger.warn("saw none value")
#         return value
#     except IndexError:
#         return default


# def try_get_lat_long(site):
#     location = None
#     try:
#         location = schema.LatLng(
#             latitude=site["geometry"]["y"],
#             longitude=site["geometry"]["x"],
#         )
#     except KeyError:
#         pass

#     return location


def normalize_state_name(name: str) -> str:

    if name is None:
        return name

    name = name.strip()
    name = name.replace(".", "")
    name = name.replace("'", "")

    # capitalize the first letter of each word in cases where a state name is provided
    spl = name.split(" ")
    if len(spl) > 1:
        " ".join([word.capitalize() for word in spl])
    else:
        name = name.lower().capitalize()

    lookup = us.states.lookup(name)
    if lookup:
        return lookup.abbr
    else:
        return name.upper()


def apply_address_fixups(address: OrderedDict[str, str]) -> OrderedDict[str, str]:

    return address


def _get_address(site):
    try:

        street_address = site.get("street_address")
        if street_address == "Locations throughout region":
            street_address = ""

        parsed = parse_address(street_address + ", " + site.get("location"))

        parsed = apply_address_fixups(parsed)

        normalized = normalize_address(parsed)

        return normalized
    except (usaddress.RepeatedLabelError, CustomBailError):
        return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site.get("name"),
        address=_get_address(site),
        location=None,
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=None,
        inventory=_get_inventory(site),
        access=_get_access(site),
        parent_organization=None,
        links=None,
        notes=_get_notes(site),
        active=None,
        source=schema.Source(
            source=SOURCE_NAME,
            id=_get_id(site),
            fetched_from_uri="https://ldh.la.gov/covidvaccine-locations",  # noqa: E501
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


# <h4>This event is ongoing Monday-Friday every week.</h4>
