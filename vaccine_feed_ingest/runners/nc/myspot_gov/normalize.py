#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import sys

from vaccine_feed_ingest_schema import location as schema

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
    ret = []
    if site["Appointment Phone"] != "":
        raw_phone = str(site["Appointment Phone"]).lstrip("1")
        if raw_phone[3] == "-" or raw_phone[7] == "-":
            phone = "(" + raw_phone[0:3] + ") " + raw_phone[4:7] + "-" + raw_phone[8:12]
            phone_notes = raw_phone[12:]
        elif len(raw_phone) == 10:
            phone = "(" + raw_phone[0:3] + ") " + raw_phone[3:6] + "-" + raw_phone[6:10]
            phone_notes = ""
        else:
            phone = raw_phone[0:14]
            phone_notes = raw_phone[14:]

        if phone_notes == "":
            ret.append(schema.Contact(phone=phone))
        else:
            phone_notes = phone_notes.lstrip(",")
            phone_notes = phone_notes.lstrip(";")
            phone_notes = phone_notes.lstrip(" ")
            ret.append(schema.Contact(phone=phone, other=f"phone_notes:{phone_notes}"))

    if site["Web Address"] != "":
        ret.append(schema.Contact(website=site["Web Address"]))
    return ret


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
    ret.append("cvms_scheduling__nc_specific:" + site["CVMS Scheduling"])
    ret.append(
        "cvms_info__nc_specific:https://covid19.ncdhhs.gov/vaccines/providers/covid-19-vaccine-management-system-cvms"
    )
    if site["Event Type"] != "" and site["Event Type"] != "Not Applicable":
        ret.append("event_type:" + site["Event Type"])
    return ret


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://myspot.nc.gov/api/get-vaccine-locations",
        id=site["Event Location Id"],
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
        inventory=_get_inventories(site),
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
