#!/usr/bin/env python3

# Adapted from rossry's tn/vaccinate_gov/normalize.py

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

from vaccine_feed_ingest_schema import location

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("ny/northwell_health/normalize.py")

SOURCE_NAME = "ny_northwell_health"


def _get_id(site: dict) -> str:
    return str(site["id"])


def _get_name(site: dict) -> str:
    # most names are of the form "Descriptive Name -- Vaccine Type"
    # we will attempt to drop the latter token where it exists
    sep = " -- "
    if sep in site["name"]:
        return " - ".join(site["name"].replace("\u2013", "-").split(sep)[:-1])
    else:
        return site["name"]


def _get_address(site: dict) -> Optional[location.Address]:
    if "address" not in site:
        return None

    return location.Address(
        street1=site["address"],
        street2=None,
        city=site["city"],
        zip=site["zip"],
        state="NY",
    )


def _get_location(site: dict) -> Optional[location.LatLng]:
    latitude = site["latitude"]
    longitude = site["longitude"]
    if latitude == "" or longitude == "":
        return None
    return location.LatLng(
        latitude=float(latitude),
        longitude=float(longitude),
    )


def _get_contacts(site: dict) -> List[location.Contact]:
    """Northwell provides a program_url, which we'll store as the contact website"""
    ret = []
    if "program_url" in site and site["program_url"]:
        program_url = str(site["program_url"])
        ret.append(location.Contact(contact_type="booking", website=program_url))
    return ret


def _get_inventories(site: dict) -> List[location.Vaccine]:
    ret = []
    if "vaccine_manufacturer" in site:
        if "Moderna" in site["vaccine_manufacturer"]:
            ret.append(location.Vaccine(vaccine="moderna"))
        if "Pfizer" in site["vaccine_manufacturer"]:
            ret.append(location.Vaccine(vaccine="pfizer_biontech"))
        if "Johnson & Johnson" in site["vaccine_manufacturer"]:
            ret.append(location.Vaccine(vaccine="johnson_johnson_janssen"))
    return ret


def _get_source(site: dict, timestamp: str) -> location.Source:
    return location.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://www.northwell.edu/coronavirus-covid-19/vaccine/locations",
        id=_get_id(site),
        source=SOURCE_NAME,
    )


def normalize(site: dict, timestamp: str) -> dict:
    normalized = location.NormalizedLocation(
        id=(f"{SOURCE_NAME}:{_get_id(site)}"),
        name=_get_name(site),
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        inventory=_get_inventories(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


def main():
    parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])
    input_files = input_dir.glob("*.ndjson")

    output_file = output_dir / "locations.normalized.ndjson"

    with output_file.open("w") as fout:
        # append normalized entries from all input files to a single output file
        for index, input_file in enumerate(input_files):
            input_file = input_dir / input_file

            with input_file.open() as parsed_lines:
                for line in parsed_lines:
                    site_blob = json.loads(line)

                    normalized_site = normalize(site_blob, parsed_at_timestamp)

                    json.dump(normalized_site, fout)
                    fout.write("\n")


if __name__ == "__main__":
    main()
