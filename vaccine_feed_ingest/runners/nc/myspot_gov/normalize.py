#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import re
import sys

from vaccine_feed_ingest.schema import schema  # noqa: E402

logger = logging.getLogger(__name__)


def _get_id(site: dict) -> str:
    loc_id = site["Event Location Id"]
    return f"nc:myspot_gov:{loc_id}"


def _get_name(site: dict) -> str:
    return site["Provider Location Name"]


def _get_address(site: dict):
    return schema.Address(
        street1=site["Street Address"],
        street2=site["Street Address 2"],
        city=site["City"],
        state=site["State"],
        zip=site["Postal Code"],
    )


def _get_location(site: dict):
    if site["latitude"] == "" or site["longitude"] == "":
        return None
    return schema.LatLng(
        latitude=float(site["latitude"]),
        longitude=float(site["longitude"]),
    )


def _get_contacts(site: dict):
    return [
        schema.Contact(
            phone=site["Appointment Phone"],
            website=site["Web Address"],
        )
    ]


def _normalize_date(dt: str):
    if dt == "":
        return None
    return dt[0:4] + "-" + dt[4:6] + "-" + dt[6:8]


def _get_opening_dates(site: dict):
    if site["Start Date"] == "" and site["End Date"]:
        return None
    return [
        schema.OpenDate(
            opens=_normalize_date(site["Start Date"]),
            closes=_normalize_date(site["End Date"]),
        )
    ]


def _get_inventories(site: dict):
    ret = []
    if site["Moderna"] == "Y":
        ret.append(schema.Vaccine(vaccine="moderna", supply_level="in_stock"))
    if site["Pfizer"] == "Y":
        ret.append(schema.Vaccine(vaccine="pfizer_biontech", supply_level="in_stock"))
    if site["Janssen"] == "Y":
        ret.append(
            schema.Vaccine(vaccine="johnson_johnson_janssen", supply_level="in_stock")
        )
    if site["Moderna"] == "N":
        ret.append(schema.Vaccine(vaccine="moderna", supply_level="out_of_stock"))
    if site["Pfizer"] == "N":
        ret.append(
            schema.Vaccine(vaccine="pfizer_biontech", supply_level="out_of_stock")
        )
    if site["Janssen"] == "N":
        ret.append(
            schema.Vaccine(
                vaccine="johnson_johnson_janssen", supply_level="out_of_stock"
            )
        )
    return ret


def _get_organization(site: dict):
    if site["Organization Name"] == "":
        return None
    if site["Organization Name"] == "Walmart, Inc.":
        return schema.Organization(name=site["Organization Name"], id="walmart")
    return schema.Organization(name=site["Organization Name"])


def _get_notes(site: dict):
    ret = []
    ret.append("cvms_scheduling:" + site["CVMS Scheduling"])
    ret.append("county:" + site["County"])
    if site["Event Type"] != "" and site["Event Type"] != "Not Applicable":
        ret.append("event_type:" + site["Event Type"])
    return ret


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://myspot.nc.gov/api/get-vaccine-locations",
        id=_get_id(site),
        source="nc:myspot_gov",
    )


def normalize(site: dict, timestamp: str) -> str:
    normalized = schema.NormalizedLocation(
        id=_get_id(site),
        name=_get_name(site),
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        opening_dates=_get_opening_dates(site),
        invetory=_get_inventories(site),
        parent_organization=_get_organization(site),
        notes=_get_notes(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "nc_data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "nc_data.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)

            normalized_site = normalize(site_blob, parsed_at_timestamp)

            json.dump(normalized_site, fout)
            fout.write("\n")
