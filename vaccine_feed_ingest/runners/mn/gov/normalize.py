#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import sys
from typing import List, Optional

from pydantic import ValidationError
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


def _get_availability(site: dict) -> Optional[schema.Availability]:
    if not site["attributes"]["TestingRequirements"]:
        return None

    requirements = site["attributes"]["TestingRequirements"].split("|")

    return schema.Availability(
        appointments=True if "Appointment" in requirements else None,
        drop_in=True if "No appointment necessary" in requirements else None,
    )


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["globalid"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "gov"
    runner = "mn"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "18279cf4ab4740ae92708db437250056"
    layer = 0

    return f"{runner}_{site_name}:{arcgis}_{layer}_{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    if phone := site["attributes"]["Phone"]:
        contacts.append(schema.Contact(phone=phone))

    def cleanup_url(url):
        if not url:
            return None
        if "@" in url:
            # Some of these are email addresses.
            # Skipping those for now.
            return None
        if url == "COVID-19 Vaccine Information - Hennepin Healthcare":
            return None
        if url == "https/:www.Lewisdrug.com":
            url = "https://www.lewisdrug.com"

        if not url.startswith("http"):
            url = "http://" + url

        return url

    if url := cleanup_url(site["attributes"]["URL"]):
        contacts.append(schema.Contact(website=url))

    if contacts:
        return contacts

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    notes = []

    if hours_info := site["attributes"]["AdditionalHoursInfo"]:
        notes.append(hours_info)

    if requirements := site["attributes"]["TestingRequirements"]:
        maybe_notes = requirements.split("|")[-1]
        if maybe_notes.count(" ") > 3:
            notes.append(maybe_notes)

    if notes:
        return notes

    return None


def _get_opening_dates(site: dict) -> Optional[List[schema.OpenDate]]:
    start_date = site["attributes"]["StartDate"]
    end_date = site["attributes"]["EndDate"]

    if start_date or end_date:
        return [schema.OpenDate(opens=start_date, closes=end_date)]

    return None


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    open_hours = []

    def _parse_time(t: str) -> datetime.time:
        # example: "8:30am"
        hh, mmxx = t.split(":")
        if hh == "12":
            hh = "0"
        return datetime.time(int(hh) + (12 if mmxx[2] == "p" else 0), int(mmxx[:2]))

    for day in [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ]:
        start = site["attributes"][f"{day}Start"]
        end = site["attributes"][f"{day}End"]
        if start and end and (start != "Closed") and (end != "Closed"):
            try:
                open_hours.append(
                    schema.OpenHour(
                        day=day.lower(),
                        opens=_parse_time(start),
                        closes=_parse_time(end),
                    )
                )
            except ValidationError:
                pass  # ignore hours that were entered incorrectly

    if open_hours:
        return open_hours

    return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["SiteName"],
        address=schema.Address(
            street1=site["attributes"]["AddrLine1"],
            street2=None,
            city=site["attributes"]["City"],
            state=site["attributes"]["State"],  # a few WI locations are included
            zip=site["attributes"]["Zip"],
        ),
        location=schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        ),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=_get_opening_dates(site),
        opening_hours=_get_opening_hours(site),
        availability=_get_availability(site),
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,
        notes=_get_notes(site),
        # The is_Active field only ever says "Active" in the current data.
        active=True if site["attributes"]["is_Active"] == "Active" else None,
        source=schema.Source(
            source="mn_gov",
            id=site["attributes"]["globalid"],
            fetched_from_uri="https://services.arcgis.com/9OIuDHbyhmH91RfZ/arcgis/rest/services/CovidVacLocations_view_prod/FeatureServer/0",  # noqa: E501
            fetched_at=timestamp,
            published_at=site["attributes"]["UpdateDate"],
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
