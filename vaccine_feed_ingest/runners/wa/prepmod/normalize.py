#!/usr/bin/env python3

import calendar
import datetime
import json
import pathlib
import re
import sys
from typing import List

from vaccine_feed_ingest.schema import schema  # noqa: E402


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        source="wa_prepmod",
        id=site["name"],
        fetched_from_uri="https://prepmod.doh.wa.gov/clinic/search",
        fetched_at=timestamp,
        data=site,
    )


def _get_inventory(site: dict) -> List[schema.Vaccine]:
    vaccines = site["vaccines"]

    inventory = []

    pfizer = re.search("pfizer", vaccines, re.IGNORECASE)
    moderna = re.search("moderna", vaccines, re.IGNORECASE)
    johnson = re.search("janssen", vaccines, re.IGNORECASE) or re.search(
        "johnson", vaccines, re.IGNORECASE
    )

    # some clinics specified all 3 vaccines but stated that they'll be given based on what's available.
    if pfizer:
        inventory.append(schema.Vaccine(vaccine="pfizer_biontech"))
    if moderna:
        inventory.append(schema.Vaccine(vaccine="moderna"))
    if johnson:
        inventory.append(schema.Vaccine(vaccine="johnson_johnson_janssen"))

    if len(inventory) == 0:
        return None

    return inventory


def _get_address(site: dict) -> schema.Address:
    address = site["address"]
    address_split = address.split(", ")

    adr2 = None if len(address_split) == 3 else address_split[1]

    return schema.Address(
        street1=address_split[0],
        street2=adr2,
        city=address_split[-2].replace(" WA", ""),
        state="WA",
        zip=address_split[-1],
    )


def _get_notes(site: dict) -> List[str]:
    return [site["info"], site["special"]]


def _get_opening_dates(site: dict) -> List[schema.OpenDate]:
    date = site["date"]
    date_split = date.split("/")

    return [
        schema.OpenDate(
            opens=f"{date_split[2]}-{date_split[0]}-{date_split[1]}",
            closes=f"{date_split[2]}-{date_split[0]}-{date_split[1]}",
        )
    ]


def _get_opening_hours(site: dict) -> List[schema.OpenHour]:
    date = site["date"]
    time = site["hours"]

    time_split = time.split(" - ")

    date_dt = datetime.datetime.strptime(date, "%m/%d/%Y")
    time_start = datetime.datetime.strptime(time_split[0], "%I:%M %p")
    time_end = datetime.datetime.strptime(time_split[1], "%I:%M %p")

    return [
        schema.OpenHour(
            day=calendar.day_name[date_dt.weekday()],
            open=time_start.strftime("%H:%M"),
            closes=time_end.strftime("%H:%M"),
        )
    ]


def normalize(site: dict, timestamp: str) -> str:
    """
    sample:
    {"name": "Rebel Med NW - COVID Vaccine Clinic", "date": "04/30/2021", "address": "5401 Leary Ave NW, Seattle WA, 98107", "vaccines": "Moderna COVID-19 Vaccine", "ages": "Adults, Seniors", "info": "truncated", "hours": "09:00 am - 05:00 pm", "available": "14", "special": "If you are signing up for a second dose, you must get the same vaccine brand as your first dose.", "clinic_id": "2731"}
    """
    normalized = schema.NormalizedLocation(
        id=f"wa_prepmod:{site['clinic_id']}",
        name=site["name"],
        address=_get_address(site),
        availability=schema.Availability(appointments=True),
        inventory=_get_inventory(site),
        opening_dates=_get_opening_dates(site),
        opening_hours=_get_opening_hours(site),
        notes=_get_notes(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site = json.loads(line)
            normalized_site = normalize(site, parsed_at_timestamp)
            json.dump(normalized_site, fout)
            fout.write("\n")
