#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import normalize_url

logger = getLogger(__file__)

try:
    from opening_hours import Times as TimeRange
except Exception:
    logger.warn(
        "if you see this message in your logs, that means a dependency changed its name in us/va_gov normalizer as predicted and its import needs updating to remove a now-unnecessary check for the old import name."
    )
    from opening_hours import TimeRange


SOURCE_NAME = "us_giscorps_vaccine_providers"


def _get_id(site: dict) -> str:
    data_id = site["id"]

    return f"{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    phs = site["attributes"].get("phone")
    if phs:
        if phs.get("main"):
            contacts.append(schema.Contact(phone=phs.get("main")))

    # there are multiple urls, vaccine, agency, health dept. etcw
    web = site["attributes"].get("website")
    if web:
        web = normalize_url(web)
        contacts.append(schema.Contact(website=web))

    if len(contacts) > 0:
        return contacts

    return None


def _get_notes(site: dict) -> Optional[List[str]]:

    notes = []
    # if site["attributes"]["operationalHoursSpecialInstructions"]:
    notes.append(site["attributes"]["operationalHoursSpecialInstructions"])
    oper_status = site["attributes"].get("additionalInfo")
    if oper_status:
        addtl = oper_status.get("additionalInfo")
        if addtl:
            notes.append(addtl)
    return notes


def _get_active(site: dict) -> Optional[bool]:

    status = site["attributes"]["operatingStatus"].get("code")

    status_options = {"NORMAL": True, "LIMITED": True, "NOTICE": False}

    return (
        try_lookup(status_options, status, None, name="active status lookup")
        or site["attributes"].get("activeStatus") != "A"
    )


def _get_access(site: dict) -> Optional[List[str]]:
    services = site["attributes"].get("detailedServices")
    walk_in = None
    walk_in_bool = None

    if services and services[0].get("name") == "COVID-19 vaccines":
        walk_in = services[0].get("walkInsAccepted")
        # referralRequired
        # onlineSchedulingAvailable

    if walk_in is None:
        pass
    elif walk_in.lower() == "true":
        walk_in_bool = True
    elif walk_in.lower() == "false":
        walk_in_bool = False
    else:
        walk_in_bool = bool(walk_in)

    return schema.Access(walk=walk_in_bool)


def try_lookup(mapping, value, default, name=None):
    if value is None:
        return default

    try:
        return mapping[value]
    except KeyError as e:
        name = " for " + name or ""
        logger.warn("value not present in lookup table%s: %s", name, e)

        return default


def _get_published_at(site: dict) -> Optional[str]:

    return None


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    days = site["attributes"].get("hours")
    openhours = []

    for day, hours in days:
        day = {
            "day": day,
        }
        # TODO: use the parse-opening hours library to parse the time range into the correct schema.
        # the library has not yet been updated to support this
        day.update(TimeRange.parse(hours))
        openhours.append(day)

    return openhours


def _get_links(site: dict) -> Optional[List[schema.Link]]:
    provider_id = site["attributes"].get("uniqueId")
    return [
        schema.Link(
            authority=None, id=provider_id, uri=site["attributes"].get("website")
        )
    ]


def try_get_list(lis, index, default=None):
    if lis is None:
        return default

    try:
        value = lis[index]
        if value == "none":
            logger.warn("saw none value")
        return value
    except IndexError:
        return default


def try_get_lat_long(site):
    location = None
    try:
        location = schema.LatLng(
            latitude=site["attributes"]["lat"],
            longitude=site["attributes"]["long"],
        )
    except KeyError:
        pass

    return location


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:

    address = site["attributes"]["address"]["physical"]

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site["attributes"]["name"],
        address=schema.Address(
            street1=address.get("address1"),
            street2=address.get("address2"),
            city=address.get("city"),
            state=address.get("state"),
            zip=address.get("zip"),
        ),
        location=try_get_lat_long(site),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=_get_opening_hours(site),
        availability=None,  # _get_availability(site),
        inventory=None,
        access=_get_access(site),
        parent_organization=None,
        links=_get_links(site),
        notes=_get_notes(site),
        active=_get_active(site),
        source=schema.Source(
            source=SOURCE_NAME,
            id=site["id"],
            fetched_from_uri="https://api.va.gov/v1/facilities/va?bbox%5B%5D=-180&bbox%5B%5D=-90&bbox%5B%5D=180&bbox%5B%5D=90&type=health&services%5B%5D=Covid19Vaccine&page=1&per_page=2000&radius=25000&latitude=37.408123149415275&longitude=-93.14343299172322",  # noqa: E501
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
