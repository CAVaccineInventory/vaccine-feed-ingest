#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import sys

from vaccine_feed_ingest_schema import schema  # noqa: E402

logger = logging.getLogger(__name__)

_source_name = "ky:govstatus"


def _get_id(site: dict) -> str:
    return site["id"]


def _get_name(site: dict) -> str:
    return site["name"]


def _get_city(site: dict) -> str:
    return site["address"]["city"]


def _get_address(site: dict):
    return schema.Address(
        street1=site["address"]["street1"],
        city=_get_city(site),
        zip=site["address"]["zip"],
        state=site["address"]["state"],
    )


def _get_location(site: dict):
    latitude = site["lat"]
    longitude = site["long"]
    if latitude == "" or longitude == "":
        return None
    return schema.LatLng(
        latitude=float(latitude),
        longitude=float(longitude),
    )


def _get_contacts(site: dict):
    ret = []

    if "register_phone" in site:
        raw_phone = site["register_phone"]
        if raw_phone != "":
            raw_phone = raw_phone.lstrip("tel:")
            raw_phone = raw_phone.lstrip(" ")
            raw_phone = raw_phone.lstrip("1")
            raw_phone = raw_phone.lstrip("-")
            raw_phone = raw_phone.lstrip(" ")
            if raw_phone[3] == "-" or raw_phone[7] == "-":
                phone = (
                    "(" + raw_phone[0:3] + ") " + raw_phone[4:7] + "-" + raw_phone[8:12]
                )
                phone_notes = raw_phone[12:]
            elif len(raw_phone) == 10:
                phone = (
                    "(" + raw_phone[0:3] + ") " + raw_phone[3:6] + "-" + raw_phone[6:10]
                )
                phone_notes = ""
            else:
                phone = raw_phone[0:14]
                phone_notes = raw_phone[14:]

            if phone_notes == "":
                ret.append(schema.Contact(phone=phone, contact_type="booking"))
            else:
                phone_notes = phone_notes.lstrip(",")
                phone_notes = phone_notes.lstrip(";")
                phone_notes = phone_notes.lstrip(" ")
                ret.append(
                    schema.Contact(
                        phone=phone,
                        other=f"phone_notes:{phone_notes}",
                        contact_type="booking",
                    )
                )

    if "register_online_url" in site:
        website = site["register_online_url"]
        if website != "":
            ret.append(schema.Contact(website=website, contact_type="booking"))
    return ret


def _get_organization(site: dict):
    # organization_name = ""
    #
    # if site["Organization Name"] == "":
    #     return None

    if _get_name(site)[0:6] == "Kroger":
        return schema.Organization(name="Kroger", id="kroger")
    if _get_name(site)[0:9] == "Walgreens":
        return schema.Organization(name="Walgreens", id="walmart")
    if _get_name(site)[0:7] == "Walmart":
        return schema.Organization(name="Walmart", id="walmart")

    # return schema.Organization(name=site["Organization Name"])


def _get_notes(site: dict):
    return []


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://govstatus.egov.com/kentucky-vaccine-map",
        id=_get_id(site),
        source=_source_name,
    )


def normalize(site: dict, timestamp: str) -> str:
    normalized = schema.NormalizedLocation(
        id=(_source_name + ":" + _get_id(site)),
        name=_get_name(site),
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        parent_organization=_get_organization(site),
        notes=_get_notes(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "govstatus.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "govstatus.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)

            normalized_site = normalize(site_blob, parsed_at_timestamp)

            json.dump(normalized_site, fout)
            fout.write("\n")
