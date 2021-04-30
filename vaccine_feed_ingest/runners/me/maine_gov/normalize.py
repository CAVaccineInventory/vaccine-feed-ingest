#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

CITY_RE = re.compile(r"^([\w ]+), NY$")
# the providerName field smells like it's being parsed from someplace else,
# a good number of them have leading \u1d42 and/or *, which we want to clean.
# there's a bunch with a city name in them, but no real pattern to it, so
# we'll leave that for now.
NAME_CLEAN_RE = re.compile("^[\u1d42*]+")

logger = logging.getLogger(__name__)


def _get_name(site: dict) -> str:
    return site["providerName"]


def _get_city(site: dict) -> str:
    return site["city"]


def _get_id(site: dict) -> str:
    name = _get_name(site)
    city = _get_city(site)
    return f"{name}:{city}"


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://www.maine.gov/covid19/vaccines/vaccination-sites",
        id=_get_id(site),
        source="me_maine_gov",
    )


def _normalize_phone(raw_phone: str) -> Optional[str]:
    raw_phone = raw_phone.lstrip("1")
    raw_phone = raw_phone.lstrip("-")
    raw_phone = raw_phone.lstrip(" ")
    if raw_phone == "":
        return None
    elif len(raw_phone) == 8:
        return "(???) " + raw_phone[0:3] + "-" + raw_phone[4:8]
    elif raw_phone[3] == "-" or raw_phone[7] == "-":
        return "(" + raw_phone[0:3] + ") " + raw_phone[4:7] + "-" + raw_phone[8:12]
    # elif len(raw_phone) == 10:
    #    return "(" + raw_phone[0:3] + ") " + raw_phone[3:6] + "-" + raw_phone[6:10]
    else:
        return raw_phone[0:14]


def _get_contacts(site: dict) -> List[schema.Contact]:
    ret = []
    for raw_phone in site["phoneNumber"]:
        general_phone = _normalize_phone(raw_phone)
        if general_phone is not None and "?" not in general_phone:
            ret.append(schema.Contact(phone=general_phone, contact_type="general"))
    for website in site["website"]:
        ret.append(schema.Contact(website=website, contact_type="general"))

    scheduling_info_raw = site["schedulingInfo"]

    website_matches = re.search('href="(http.*)"', scheduling_info_raw)
    if website_matches:
        website = website_matches.group(1)
    else:
        website = None

    phone_matches = re.search(
        "tel:([-() \\d]*)", scheduling_info_raw.replace("\u2013", "-")
    )  # .replace() replaces en dash with ASCII '-', for better regex
    if phone_matches:
        raw_phone = phone_matches.group(1)
    else:
        phone_matches = re.search(
            "(\\d\\d\\d-\\d\\d\\d-\\d\\d\\d\\d)",
            scheduling_info_raw.replace("\u2013", "-"),
        )  # .replace() replaces en dash with ASCII '-', for better regex
        if phone_matches:
            raw_phone = phone_matches.group(1)
        elif "1-800-Walgreens" in scheduling_info_raw:
            raw_phone = "(800) 925-4733"
        else:
            raw_phone = ""

    booking_phone = _normalize_phone(raw_phone)
    if booking_phone is not None and "?" not in booking_phone:
        ret.append(schema.Contact(contact_type="booking", phone=booking_phone))

    if website is not None:
        ret.append(schema.Contact(contact_type="booking", website=website))

    ret.append(schema.Contact(contact_type="booking", other=scheduling_info_raw))

    return ret


def _get_organization(site: dict) -> Optional[schema.Organization]:
    if _get_name(site) == "Walmart":
        return schema.Organization(name=_get_name(site), id="walmart")
    if _get_name(site) == "Walgreens":
        return schema.Organization(name=_get_name(site), id="walgreens")
    return None


def _get_notes(site: dict) -> List[str]:
    ret = []
    ret.append("city:" + site["city"])
    ret.append("county:" + site["county"])
    if site["minimumAge"] != "":
        ret.append("minimum_age:" + site["minimumAge"])
    if site["audience"] != "":
        ret.append("audience:" + site["audience"])
    return ret


def normalize(site: dict, timestamp: str) -> str:
    normalized = schema.NormalizedLocation(
        id=("me_maine_gov:" + _get_id(site)),
        name=_get_name(site),
        contact=_get_contacts(site),
        source=_get_source(site, timestamp),
        parent_organization=_get_organization(site),
        notes=_get_notes(site),
    ).dict()
    return normalized


def normalize_from_list(sites: list, timestamp: str) -> List[str]:
    ret = []
    for site in sites:
        ret.append(normalize(site, timestamp))
    return ret


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)

            normalized_sites = []
            if isinstance(site_blob, list):
                normalized_sites = normalize_from_list(site_blob, parsed_at_timestamp)
            else:
                normalized_sites = normalize_from_list([site_blob], parsed_at_timestamp)

            for normalized_site in normalized_sites:
                json.dump(normalized_site, fout)
                fout.write("\n")
