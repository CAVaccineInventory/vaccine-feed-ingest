#!/usr/bin/env python

# this dataset produces duplicates when a single location offers multiple
# vaccine types, or offers both first and second doses. we make no effort to
# deduplicate them here, though lat/long should be sufficient to do so
# downstream.

import datetime
import json
import pathlib
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

SOURCE_NAME = "az_ph_pinal_clinics_gov"


def _get_id(site: dict) -> str:
    name = _get_name(site)
    city = _get_city(site)

    id = f"{name}_{city}".lower()
    id = id.replace(" ", "_").replace("\u2019", "_")
    id = id.replace(".", "_").replace(",", "_").replace("'", "_")
    id = id.replace("(", "_").replace(")", "_").replace("/", "_")

    return id


def _get_city(site: dict) -> str:
    return site["City"]


def _get_name(site: dict) -> str:
    return site["Clinic"]


def _get_address(site: dict) -> Optional[schema.Address]:
    raw_address = site["Address"]

    zip = raw_address[-5:]
    raw_address = raw_address.rstrip(zip)
    raw_address = raw_address.rstrip().rstrip(",").rstrip()

    raw_address = raw_address.rstrip("AZ")
    raw_address = raw_address.rstrip().rstrip(",").rstrip()

    raw_address = raw_address.rstrip(_get_city(site))
    raw_address = raw_address.rstrip().rstrip(",").rstrip()

    address_lines = raw_address.split(", ")

    return schema.Address(
        street1=address_lines[0],
        street2=(", ".join(address_lines[1:]) if len(address_lines) > 1 else None),
        city=_get_city(site),
        zip=zip,
        state=schema.State.ARIZONA,
    )


def _get_contacts(site: dict) -> List[schema.Contact]:
    pn = site["Phone Number"]
    return [schema.Contact(phone=f"({pn[2:5]}) {pn[6:9]}-{pn[10:14]}")]


def _get_open_hours(site: dict) -> List[schema.OpenHour]:
    raw_hours = site["WIC Hours"]

    raw_times = raw_hours.split(" ")[-1]
    raw_open = raw_times.split("-")[0]
    opens = (
        f"{raw_open[:-2]}:00:00"
        if raw_open[-2:] == "am"
        else f"{int(raw_open[:-2]) + 12}:00:00"
    )
    raw_close = raw_times.split("-")[1]
    closes = (
        f"{raw_close[:-2]}:00:00"
        if raw_close[-2:] == "am"
        else f"{int(raw_close[:-2]) + 12}:00:00"
    )

    raw_hours = " ".join(raw_hours.split(" ")[:-1])
    days_of_week = []
    if raw_hours[-12:] == "of the Month":
        days_of_week = [raw_hours.split(" ")[-4].lower()]
    elif "to" in raw_hours:
        start_day_of_week = raw_hours.split(" to ")[0].lower()
        end_day_of_week = raw_hours.split(" to ")[1].lower()

        in_range = False
        for day in [
            schema.DayOfWeek.SUNDAY,
            schema.DayOfWeek.MONDAY,
            schema.DayOfWeek.TUESDAY,
            schema.DayOfWeek.WEDNESDAY,
            schema.DayOfWeek.THURSDAY,
            schema.DayOfWeek.FRIDAY,
            schema.DayOfWeek.SATURDAY,
        ]:
            if day == start_day_of_week:
                in_range = True
            if in_range:
                days_of_week.append(day)
            if day == end_day_of_week:
                in_range = False
    else:
        days_of_week = [raw_hours.rstrip("s").lower()]

    return [
        schema.OpenHour(day=day, opens=opens, closes=closes) for day in days_of_week
    ]


# may encode Nth-day_of_week info that current schema can't represent
def _get_notes(site: dict) -> List[str]:
    hours_note = site["WIC Hours"]
    return [f"Hours: {hours_note}"]


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://www.pinalcountyaz.gov/publichealth/Pages/OfficeLocations.aspx",
        id=_get_id(site),
        source=SOURCE_NAME,
    )


def normalize(site: dict, timestamp: str) -> dict:
    normalized = schema.NormalizedLocation(
        id=(f"{SOURCE_NAME}:{_get_id(site)}"),
        name=_get_name(site),
        address=_get_address(site),
        contact=_get_contacts(site),
        opening_hours=_get_open_hours(site),
        notes=_get_notes(site),
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
