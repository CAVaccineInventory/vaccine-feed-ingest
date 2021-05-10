#!/usr/bin/env python3

import calendar
import datetime
import json
import logging
import os
import pathlib
import re
import sys
import urllib.parse
from typing import List

import yaml
from vaccine_feed_ingest_schema import location as schema

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("_shared/parse.py")

OUTPUT_DIR = pathlib.Path(sys.argv[1])
INPUT_DIR = pathlib.Path(sys.argv[2])
YML_CONFIG = pathlib.Path(sys.argv[3])


def _get_config(yml_config: pathlib.Path) -> dict:
    with open(yml_config, "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config


def _get_source(config: dict, site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        source=config["site"],
        id=site["clinic_id"],
        fetched_from_uri=urllib.parse.urljoin(config["url"], "clinic/search"),
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
        city=address_split[-2].replace(f" {config['state'].upper()}", ""),
        state=config["state"].upper(),
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
            day=calendar.day_name[date_dt.weekday()].lower(),
            opens=time_start.strftime("%H:%M"),
            closes=time_end.strftime("%H:%M"),
        )
    ]


def _get_contact(config: dict, site: dict) -> List[schema.Contact]:
    return [
        schema.Contact(
            contact_type="booking",
            website=f"{config['url']}/appointment/en/client/registration?clinic_id={site['clinic_id']}",
        )
    ]


def _get_out_filepath(in_filepath: pathlib.Path, out_dir: pathlib.Path) -> pathlib.Path:
    filename, _ = os.path.splitext(in_filepath.name)
    return out_dir.joinpath(f"{filename}.normalized.ndjson")


def normalize(config: dict, site: dict, timestamp: str) -> str:
    """
    sample:
    {"name": "Rebel Med NW - COVID Vaccine Clinic", "date": "04/30/2021", "address": "5401 Leary Ave NW, Seattle WA, 98107", "vaccines": "Moderna COVID-19 Vaccine", "ages": "Adults, Seniors", "info": "truncated", "hours": "09:00 am - 05:00 pm", "available": "14", "special": "If you are signing up for a second dose, you must get the same vaccine brand as your first dose.", "clinic_id": "2731"} # noqa: E501
    """
    normalized = schema.NormalizedLocation(
        id=f"{config['site']}:{site['clinic_id']}",
        name=site["name"],
        address=_get_address(site),
        availability=schema.Availability(appointments=True),
        contact=_get_contact(config, site),
        inventory=_get_inventory(site),
        opening_dates=_get_opening_dates(site),
        opening_hours=_get_opening_hours(site),
        notes=_get_notes(site),
        source=_get_source(config, site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

config = _get_config(YML_CONFIG)

if config["parser"] == "prepmod":
    for input_file in INPUT_DIR.glob("*.ndjson"):
        output_file = _get_out_filepath(input_file, OUTPUT_DIR)
        with input_file.open() as parsed_lines:
            with output_file.open("w") as fout:
                for line in parsed_lines:
                    site = json.loads(line)
                    normalized_site = normalize(config, site, parsed_at_timestamp)
                    json.dump(normalized_site, fout)
                    fout.write("\n")
