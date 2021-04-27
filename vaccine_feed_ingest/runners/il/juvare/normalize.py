#!/usr/bin/env python3
# parts adapted from ok/vaccinate_gov/normalize.py

import datetime
import json
import pathlib
import re
import sys
from typing import List

from vaccine_feed_ingest.schema import schema  # noqa: E402


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        source="il_juvare",
        id=site["slug"],
        fetched_from_uri="https://covidvaccination.dph.illinois.gov/",
        fetched_at=timestamp,
        data=site,
    )


def _parse_date(date: str) -> str:
    match = re.match(r"(\d+)/(\d+)/(\d+).*", date or "")
    if not match:
        return None
    month, day, year = [int(x) for x in match.groups()]
    return f"{year:04}-{month:02}-{day:02}"


def _get_opening_dates(site: dict) -> schema.OpenDate:
    return [
        schema.OpenDate(
            opens=_parse_date(site["dateFrom"]), closes=_parse_date(site["dateTo"])
        )
    ]


def _get_access(site: dict) -> schema.Access:
    if re.search(r"\bdrive[- ](thru|up)\b", site["name"], flags=re.IGNORECASE):
        return schema.Access(drive=True)
    return None


def _get_building_and_address(site: dict) -> (str, schema.Address):
    # NOTE: some locations are missing the ZIP code. We return None for these
    # locations because the schema requires a ZIP code.

    # The street name often runs together with the city name, like this:
    # "3601 W 183rd St Hazel Crest IL 60429".
    # We look for punctuation or a street type (like "St") at the end of the
    # street name.

    match = re.match(
        r"""
        (?P<building>[^0-9]*)        # building name
        (?P<street>[0-9].*           # street address, must end with punctuation or street type
          (\b(ave|avenue|blvd|boulevard|cir|circle|ct|court|dr|drive|hwy|highway|ln|lane|pkwy|parkway|st|street|way)\b\s*|[.,0-9]\s*|[\r\n])
        )
        (?P<city>(\b\w+[ ]*)+)       # city
        [,]?\s+(IL|Illinois)\b[.,]?  # state
        \s+(?P<zip>\d{5})(-\d{4})?   # zip
        (?P<extra>.*)                # building name, instructions, county name, "united states", etc.
        """,
        site["location"],
        re.DOTALL | re.IGNORECASE | re.VERBOSE,
    )

    if not match:
        return ("", None)

    address = schema.Address(
        street1=match.group("street").strip(" ,\r\n"),
        city=match.group("city"),
        state="IL",
        zip=match.group("zip"),
    )
    building = match.group("building") or match.group("extra")
    if re.match(
        r".*\b(county|united states|enter off crystal lake)\b",
        building,
        flags=re.IGNORECASE,
    ):
        building = ""
    building = building.strip(" ,\r\n@.")
    return (building, address)


def _get_contact(site: dict) -> schema.Contact:
    indirect_url = "https://covidvaccination.dph.illinois.gov/"

    # NOTE: This direct url bypasses the consent form used at the indirect url,
    # so we don't actually return it.
    # direct_url = f"https://events.juvare.com/{site['organizer']}/{site['slug']}/"

    return [schema.Contact(contact_type="booking", website=indirect_url)]


def _get_inventory(site: dict) -> List[schema.Vaccine]:
    name = site["name"]

    inventory = []

    pfizer = re.search("pfizer", name, re.IGNORECASE)
    moderna = re.search("moderna", name, re.IGNORECASE)
    johnson = re.search("janssen|johnson.*johnson|j&j", name, re.IGNORECASE)

    if pfizer:
        inventory.append(
            schema.Vaccine(vaccine="pfizer_biontech", supply_level="in_stock")
        )
    elif moderna:
        inventory.append(schema.Vaccine(vaccine="moderna", supply_level="in_stock"))
    elif johnson:
        inventory.append(
            schema.Vaccine(vaccine="johnson_johnson_janssen", supply_level="in_stock")
        )

    if len(inventory) == 0:
        return None

    return inventory


def _filter_name(building: str, site: dict) -> str:
    if building:
        # The building name from the address is almost always better than the
        # name from the "name" field. For example, the address has "West
        # Prairie High School" when the name field has "McDonough - (WPHS)".
        return re.sub(r"\r?\n", " - ", building)

    name = site["name"]
    possible_names = [name]

    # Remove dates from beginning of name.
    name = re.sub(
        r"""^(
            [-/,& 0-9\xa0\u2013]
            | [a-z]*day\b        # Monday, etc.
            | th\b               # 24th, etc.
            | \b(
                and|to
                |jan(uary)?|feb(ruary)?|mar(ch)?|apr(il)?|may|june?
                |july?|aug(ust)?|sep(tember)?|oct(ober)?|nov(ember)?|dec(ember)?
              )
            \b)+""",
        "",
        name,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    possible_names.append(name)

    # Remove dose counts ("120 doses") and numbers ("1st and 2nd doses",
    # "dose 1").
    name = re.sub(
        r"""\b\d+\s+doses\b
            |\bfor\s+those\s+that\s+received.*
            |\b((1st|first|2nd|second|single|and|or|&)\s+)+doses?\b
            |\bdoses?\s+(\s+|[#]|[12]|and|or|&)+\b
            |\b(1st|first|2nd|second)\b
            |[#][12]\b""",
        "",
        name,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    possible_names.append(name)

    # Remove vaccine types.
    name = re.sub(
        r"""\b(j&j|johnson\s+(and|&)\s+johnson|moderna|pfizer)\b""",
        "",
        name,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    possible_names.append(name)

    # Remove county names. Note that this doesn't modify things like "Vermilion
    # County Health Department".
    name = re.sub(
        r"""^(([-a-z]+,?|&)\s+)+(bi)?count(y|ies)\s*[-\u2013]""",
        "",
        name,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    possible_names.append(name)

    # Find the shortest name that looks reasonable.
    possible_names.reverse()
    for name in possible_names:
        name = name.strip(" -\xa0")
        name = re.sub(r"(\s+[-]|[-]\s+)", r" \1 ", name)
        name = re.sub(r":\s+-", ": ", name)
        name = re.sub(r"\s+", " ", name)
        # Short names are probably just "Clinic" or "Vaccine".
        if len(name) > 7:
            return name

    return name


def normalize(site: dict, timestamp: str) -> str:
    """
    input keys:
    - "organizer": Always "IL-IDPH".
    - "slug": Unique ID, may be a UUID or something like "7vbvl-29-31". Used in URLs.
    - "name": Name, often includes dates and vaccine type but no consistent format.
    - "location": Street address, no consistent format.
    - "dateFrom": Start time, always formatted like "4/24/2021, 9:30 AM".
    - "dateTo": End time, may be null, otherwise always formatted like "4/24/2021, 4:30 PM".
    - "lat": Latitude, may be null.
    - "lon": Longitude, may be null.
    - "search": Not interesting, just a bunch of the other fields joined together.
    """
    building, address = _get_building_and_address(site)

    if site["lat"] and site["lon"]:
        location = {"latitude": site["lat"], "longitude": site["lon"]}
    else:
        location = None

    normalized = schema.NormalizedLocation(
        id=f"il_juvare:{site['slug']}",
        name=_filter_name(building, site),
        address=address,
        location=location,
        contact=_get_contact(site),
        opening_dates=_get_opening_dates(site),
        availability=schema.Availability(appointments=True),
        inventory=_get_inventory(site),
        access=_get_access(site),
        active=True,
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "events.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "events.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site = json.loads(line)
            normalized_site = normalize(site, parsed_at_timestamp)
            json.dump(normalized_site, fout)
            fout.write("\n")
