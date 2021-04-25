#!/usr/bin/env python3
# parts adapted from ny/am_i_eligible_covid19vaccine_gov/normalize

import datetime
import json
import pathlib
import re
import sys

from vaccine_feed_ingest.schema import schema  # noqa: E402


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


def _get_contact(site: dict) -> schema.Contact:
    PHONE_RE = re.compile(r"(?<=o: )(.*)(?= <)")
    PHONE_FORMAT_RE = re.compile(r"([0-9]{3}).{0,1}([0-9]{3}).{0,1}([0-9]{4})")

    raw_address = site["Description"]
    sections = raw_address.split("\r\n")

    contact_section = sections[-1]
    phone_num_search = PHONE_RE.search(contact_section)

    if phone_num_search is None:
        return None

    # sometimes the numbers come formatted in some way - the replacement is undoing that formatting
    phone_num_string = (
        phone_num_search.group(1)
        .replace("(", "")
        .replace(")", "")
        .replace("\xa0", " ")
        .replace(" ", "-")
    )
    phone_num_format = PHONE_FORMAT_RE.search(phone_num_string)

    # less than 10 digit number
    if phone_num_format is None:
        return None

    phone_num = f"({phone_num_format.group(1)}) {phone_num_format.group(2)}-{phone_num_format.group(3)}"

    return [schema.Contact(contact_type="general", phone=phone_num)]


def normalize(site: dict, timestamp: str) -> str:
    """
    sample:
    {"Description": "608 NW 9th St\r\nSuite 1100\r\nOklahoma City, Oklahoma 73102 <br>Phone No: 405-425-4489 <br> ", "Distance": 0.6, "Id": "6953713d-7e6a-eb11-a812-001dd800ac9e", "Latitude": 35.47689, "Location": "35.47689,-97.52351", "Longitude": -97.52351, "PushpinImageHeight": 39, "PushpinImageUrl": "", "PushpinImageWidth": 32, "Title": "1st Dose- Pfizer- OKC- SSM Health Family Medicine Center", "Url": ""} # noqa: E501
    """
    normalized = schema.NormalizedLocation(
        id=f"ok_vaccinate_gov:{site['Id']}",
        name=site["Title"],
        address=_get_address(site),
        location={"latitude": site["Latitude"], "longitude": site["Longitude"]},
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
