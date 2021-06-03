#!/usr/bin/env python3
# parts adapted from ny/am_i_eligible_covid19vaccine_gov/normalize

import datetime
import json
import pathlib
import re
import sys
from typing import List

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.normalize import normalize_phone


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        source="ok_vaccinate_gov",
        id=site["Id"],
        fetched_from_uri="https://vaccinate.oklahoma.gov/en-US/vaccine-centers/",
        fetched_at=timestamp,
        data=site,
    )


def _get_address(site: dict) -> schema.Address:
    CITY_RE = re.compile(r"^(.+?),")
    ZIP_RE = re.compile(r"\s+(\d{5})")

    raw_address = site["Description"]
    sections = raw_address.split("\r\n")

    # sometimes they just use \n instead of \r\n
    if len(sections) == 1:
        sections = raw_address.split("\n")

    # if there still isn't more than one section, it's likely there isn't an address for this loc
    if len(sections) == 1:
        return None

    adr2 = sections[1] if len(sections) == 3 else None
    csz_sec = sections[1] if adr2 is None else sections[2]

    adr1 = sections[0]
    city = CITY_RE.search(csz_sec).group(1)
    zip_search = ZIP_RE.search(csz_sec)

    zipc = None if zip_search is None else zip_search.group(1)

    # no zip, no valid address
    if zipc is None:
        return None
    else:
        return schema.Address(
            street1=adr1, street2=adr2, city=city, state="OK", zip=zipc
        )


def _get_contact(site: dict) -> List[schema.Contact]:
    contacts = []
    for phone in normalize_phone(site["Description"], "general"):
        contacts.append(phone)
    if contacts:
        return contacts
    return None


def _get_inventory(site: dict) -> List[schema.Vaccine]:
    name = site["Title"]

    inventory = []

    pfizer = re.search("pfizer", name, re.IGNORECASE)
    moderna = re.search("moderna", name, re.IGNORECASE)
    johnson = re.search("janssen", name, re.IGNORECASE) or re.search(
        "johnson", name, re.IGNORECASE
    )

    if pfizer:
        inventory.append(schema.Vaccine(vaccine="pfizer_biontech"))
    elif moderna:
        inventory.append(schema.Vaccine(vaccine="moderna"))
    elif johnson:
        inventory.append(schema.Vaccine(vaccine="johnson_johnson_janssen"))

    if len(inventory) == 0:
        return None

    return inventory


def _filter_name(site: dict) -> str:
    name = site["Title"]

    # various terms that are being used before the location
    search_for = ["dose", "moderna", "pfizer", "johnson and johnson", "janssen", "only"]
    largest_end_val = 0
    largest_end_term = "dose"

    for term in search_for:
        reg = re.compile("^.*" + term, flags=re.I)
        search = reg.search(name)

        if search is None:
            continue

        num = search.end()
        if num > largest_end_val:
            largest_end_val = num
            largest_end_term = term

    if largest_end_val is None:
        return name

    replaced = re.sub(f"^.*{largest_end_term}", "", name, flags=re.IGNORECASE)
    clean = re.sub(
        r"^([a-zA-Z\d:]{0,1}[^a-zA-Z\d:]+?)([a-z,A-Z]{2})",
        "\\2",
        replaced,
        flags=re.IGNORECASE,
    )

    # this is arbitrary but if the resulting string is less than 5 chars it probably makes sense to use the original
    if len(clean) <= 5:
        return name

    return clean


def normalize(site: dict, timestamp: str) -> str:
    """
    sample:
    {"Description": "608 NW 9th St\r\nSuite 1100\r\nOklahoma City, Oklahoma 73102 <br>Phone No: 405-425-4489 <br> ", "Distance": 0.6, "Id": "6953713d-7e6a-eb11-a812-001dd800ac9e", "Latitude": 35.47689, "Location": "35.47689,-97.52351", "Longitude": -97.52351, "PushpinImageHeight": 39, "PushpinImageUrl": "", "PushpinImageWidth": 32, "Title": "1st Dose- Pfizer- OKC- SSM Health Family Medicine Center", "Url": ""} # noqa: E501
    """
    normalized = schema.NormalizedLocation(
        id=f"ok_vaccinate_gov:{site['Id']}",
        name=_filter_name(site),
        address=_get_address(site),
        location={"latitude": site["Latitude"], "longitude": site["Longitude"]},
        inventory=_get_inventory(site),
        contact=_get_contact(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "output.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "output.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site = json.loads(line)
            normalized_site = normalize(site, parsed_at_timestamp)
            json.dump(normalized_site, fout)
            fout.write("\n")
