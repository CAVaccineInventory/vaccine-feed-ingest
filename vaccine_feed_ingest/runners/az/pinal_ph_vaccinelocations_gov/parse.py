#!/usr/bin/env python3

import json
import pathlib
import re
import sys

from bs4 import BeautifulSoup

CITY_REGEX = re.compile(r"\((\S+) ?- ?\S+ County\)")
PHONE_REGEX = re.compile(r".*?((?:1-? )?\(?\d{3}\)?-? ?\d{3}-? ?\d{4}(?: x\d+)?).*?")

JANSSEN_REGEX = re.compile(r".*Janssen.*")
MODERNA_REGEX = re.compile(r".*Moderna.*")
PFIZER_REGEX = re.compile(r".*Pfizer.*")


def extractAddress(s):
    candidates = s.select('a[href^="https://www.google.com/maps/"]')
    for c in candidates:
        address = c.string.strip()
        address = address.replace("\n", "")
        if address.startswith("Dexcom"):  # Special case, they don't give the address.
            address = "232 S Dobson Rd, Mesa, AZ 85202"

        return address
    return None


def extractCity(t):
    city = t

    # Try to extract city from AZ Dept. Health Services city names,
    # e.g. "(Mesa-Maricopa County)".
    m = CITY_REGEX.match(city)
    if m is not None and m.group(1) is not None:
        city = m.group(1)

    return city


def extractPhone(t):
    """Try to extract a phone number from the comment field."""
    if not t:
        return None

    m = PHONE_REGEX.match(t)
    if m:
        return m.group(1)

    return None


def extractVaccineType(t):
    """Try to extract the vaccine type from the comment field."""
    types = []
    if t:
        if JANSSEN_REGEX.match(t):
            types.append("Janssen")
        if MODERNA_REGEX.match(t):
            types.append("Moderna")
        if PFIZER_REGEX.match(t):
            types.append("Pfizer")
    return types


input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])

input_file = input_dir / "vaccinelocations.html"
output_file = output_dir / "data.parsed.ndjson"

with input_file.open() as fin:
    content = fin.read()
    sites = []
    soup = BeautifulSoup(content, "html5lib")
    table = soup.find(id="table")

    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")
        site_data = {
            "providerName": cells[1].string.strip(),
            "city": extractCity(cells[3].string),
        }

        address = extractAddress(cells[2])
        if address:
            site_data["address"] = address.strip()

        if len(cells) >= 5:  # Some rows aren't fully populated.
            website = cells[4].find("a")
            if website:
                site_data["website"] = website["href"]

            vaccineType = extractVaccineType(cells[4].string)
            if vaccineType:
                site_data["vaccineType"] = vaccineType

        if len(cells) >= 6:  # Some rows aren't fully populated.
            pn = extractPhone(cells[5].string)
            if pn:
                site_data["phoneNumber"] = pn

        sites.append(site_data)

    with open(output_file, "w") as fout:
        for site in sites:
            json.dump(site, fout)
            fout.write("\n")
