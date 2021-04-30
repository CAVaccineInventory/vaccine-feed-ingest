#!/usr/bin/env python3
# parts adapted from ok/vaccinate_gov/normalize.py

import datetime
import json
import pathlib
import re
import sys
from typing import List, Optional, Tuple

from vaccine_feed_ingest_schema import location  # noqa: E402


def _get_source(site: dict, timestamp: str) -> location.Source:
    return location.Source(
        source="il_juvare",
        id=site["slug"],
        fetched_from_uri="https://covidvaccination.dph.illinois.gov/",
        fetched_at=timestamp,
        data=site,
    )


def _parse_date(date: str) -> Optional[str]:
    # Input data always looks like "3/23/2021, 9:00 AM", unless it's null.
    match = re.match(r"(\d+)/(\d+)/(\d+).*", date or "")
    if not match:
        return None
    month, day, year = [int(x) for x in match.groups()]
    return f"{year:04}-{month:02}-{day:02}"


def _get_opening_dates(site: dict) -> location.OpenDate:
    return [
        location.OpenDate(
            opens=_parse_date(site["dateFrom"]), closes=_parse_date(site["dateTo"])
        )
    ]


def _get_access(site: dict) -> location.Access:
    if re.search(r"\bdrive[- ](thru|up)\b", site["name"], flags=re.IGNORECASE):
        return location.Access(drive=True)
    return None


def _get_building_and_address(site: dict) -> Tuple[str, Optional[location.Address]]:
    # Input has no consistent format. Examples:
    # - ""
    # - "100 E. Jeffery St. Kankakee, IL 60901"
    # - "100 S Main St, Crystal Lake, IL 60014 (Enter off Crystal Lake Ave.)"
    # - "3330 W 177th St \r\nSuite 3F\r\nHazel Crest IL\r\nEmerge MedStaffing"
    # - "Bushnell-Prairie City High School\r\n845 N Walnut St\r\nBushnell, IL 61422"

    # The street name often runs together with the city name, like this:
    # - "3601 W 183rd St Hazel Crest IL 60429".
    # We look for punctuation or a street type (like "St") at the end of the
    # street name.

    # If there's extra text before or after the address, like "Emerge
    # MedStaffing" or "Bushnell-Prairie City High School", we remove it from
    # the address and return it separately. It's probably a building name, and
    # the _filter_name() function may use it as the name of the location.

    match = re.match(
        r"""
        (?P<building>[^0-9]*)          # building name
        (?P<street>[0-9].*             # street address, must end with punctuation or street type
            (\b(ave|avenue|blvd|boulevard|cir|circle|ct|court|dr|drive|hwy|highway|ln|lane|pkwy|parkway|st|street|way)\b\s*|[.,0-9]\s*|[\r\n])
        )
        (?P<city>(\b\w+[ ]*)+)         # city
        [,]?\s+(IL|Illinois)\b[.,]?    # state
        (\s+(?P<zip>\d{5}(-\d{4})?))?  # zip
        (?P<extra>.*)                  # building name, instructions, county name, "united states", etc.
        """,
        site["location"],
        re.DOTALL | re.IGNORECASE | re.VERBOSE,
    )

    if not match:
        return ("", None)

    address = location.Address(
        street1=match.group("street").strip(" ,\r\n"),
        city=match.group("city"),
        state=location.State.ILLINOIS,
        zip=match.group("zip"),
    )
    building = match.group("building") or match.group("extra")
    if re.match(
        r".*\b(county|united states|enter off crystal lake)\b",
        building,
        flags=re.IGNORECASE,
    ):
        # Not a building name.
        building = ""
    building = building.strip(" ,\r\n@.")
    return (building, address)


def _get_contact(site: dict) -> location.Contact:
    # indirect_url = "https://covidvaccination.dph.illinois.gov/"

    # NOTE: This direct url bypasses the consent form used at the indirect url,
    # but it's very easy for people to find the direct url by following links
    # from provider websites and search results, so it should be okay to return
    # it.
    direct_url = f"https://events.juvare.com/{site['organizer']}/{site['slug']}/"

    return [
        location.Contact(contact_type=location.ContactType.BOOKING, website=direct_url)
    ]


def _get_inventory(site: dict) -> Optional[List[location.Vaccine]]:
    name = site["name"]

    inventory = []

    pfizer = re.search("pfizer", name, re.IGNORECASE)
    moderna = re.search("moderna", name, re.IGNORECASE)
    johnson = re.search("janssen|johnson.*johnson|j&j", name, re.IGNORECASE)

    # The source only seems to list locations that have available appointment
    # times, so they most likely have stock available.
    if pfizer:
        inventory.append(
            location.Vaccine(
                vaccine=location.VaccineType.PFIZER_BIONTECH,
                supply_level=location.VaccineSupply.IN_STOCK,
            )
        )
    if moderna:
        inventory.append(
            location.Vaccine(
                vaccine=location.VaccineType.MODERNA,
                supply_level=location.VaccineSupply.IN_STOCK,
            )
        )
    if johnson:
        inventory.append(
            location.Vaccine(
                vaccine=location.VaccineType.JOHNSON_JOHNSON_JANSSEN,
                supply_level=location.VaccineSupply.IN_STOCK,
            )
        )

    if len(inventory) == 0:
        return None

    return inventory


def _filter_name(building: str, site: dict) -> str:
    # Inputs loosely follow the format "dates - county - name - vaccine type",
    # but there are lots of inconsistencies. Examples:
    # - "04/01/2021 – Will County – State Mass Vaccination Site"
    # - "04/19 - 08/31 Emerge MedStaffing - Moderna 1st and 2nd Dose"
    # - "05/23/2021 - Marion County - ILNG Rural Outreach Vaccination Clinic - Moderna 2nd Dose - J&J"
    # - "Friday 4/30 -McHenry County-McHenry County Residents: PFIZER 1ST AND 2ND DOSES-McHenry, IL"
    # - "May 11 to May 21 - Fayette County  - Health Dept 1st and 2nd dose Moderna Clinic"
    # - "May 1-4, 2021 \"It's Your Turn\" Knox County Unified Command Covid-19 Vaccine- 1st Dose"

    if building:
        # When there's a building name in the address, it's almost always
        # better than the more generic name from the "name" field. For example,
        # the address has "West Prairie High School" when the name field has
        # "McDonough - (WPHS)".
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
        r"""\b(j&j|janssen|johnson\s+(and|&)\s+johnson|moderna|pfizer)\b""",
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
        name = re.sub(r"([-:/])(\s+[-:/])+", r"\1 ", name)
        name = re.sub(r"\s+", " ", name)
        name = name.strip(" -:/")
        # Really short names are probably just "Clinic" or "Vaccine".
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
        latlng: Optional[location.LatLng] = location.LatLng(
            latitude=site["lat"], longitude=site["lon"]
        )
    else:
        latlng = None

    normalized = location.NormalizedLocation(
        id=f"il_juvare:{site['slug'].replace('-','_')}",
        name=_filter_name(building, site),
        address=address,
        location=latlng,
        contact=_get_contact(site),
        opening_dates=_get_opening_dates(site),
        availability=location.Availability(appointments=True),
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
