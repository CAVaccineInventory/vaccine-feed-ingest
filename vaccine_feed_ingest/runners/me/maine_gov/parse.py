#!/usr/bin/env python

import json
import pathlib
import sys

from bs4 import BeautifulSoup


def extract_age(s):
    """Regularize age data."""
    if s in ["16+", "Yes"]:
        return "16"
    if s in ["No"]:
        return "18"
    return ""


def extract_phone(s):
    """Extract phone numbers from scheduling information."""
    phones = []
    candidates = s.select('a[href^="tel:"]')
    for c in candidates:
        phones.append(c.string)
    return phones


def extract_website(s):
    """Extract websites from scheduling information."""
    websites = []
    candidates = s.select('a[href^="http"]')
    for c in candidates:
        websites.append(c["href"])
    return websites


def extract_healthcenters(table):
    """Extract data for health centers."""
    data = []
    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")
        site_data = {
            "providerName": cells[0].string,
            "city": cells[1].string,
            "county": cells[2].string,
            "schedulingInfo": str(cells[3])[4:-5],  # Skip <td> & </td> framing.
            "minimumAge": extract_age(cells[4].string),
            "audience": cells[5].string,
            "phoneNumber": extract_phone(cells[3]),
            "website": extract_website(cells[3]),
        }
        data.append(site_data)
    return data


def extract_pharmacies(table):
    """Extract data for pharmacies."""
    data = []
    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")
        site_data = {
            "providerName": cells[0].string,
            "city": cells[1].string,
            "county": cells[2].string,
            "schedulingInfo": str(cells[3])[4:-5],  # Skip <td> & </td> framing.
            "minimumAge": extract_age(cells[4].string),
            "audience": "Public",
            "phoneNumber": extract_phone(cells[3]),
            "website": extract_website(cells[3]),
        }
        data.append(site_data)
    return data


def extract_mobile(table):
    """Extract data for mobile sites."""
    data = []
    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")
        site_data = {
            "providerName": cells[0].string,
            "city": cells[1].string,
            "county": cells[2].string,
            "schedulingInfo": str(cells[3])[4:-5],  # Skip <td> & </td> framing.
            "minimumAge": extract_age(cells[4].string),
            "audience": cells[5].string,
            "phoneNumber": extract_phone(cells[3].string),
            "website": extract_website(cells[3].string),
        }
        data.append(site_data)
    return data


input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])

input_file = input_dir / "maine_gov.html"
output_file = output_dir / "data.parsed.ndjson"

with input_file.open() as fin:
    content = fin.read()
    sites = []
    soup = BeautifulSoup(content, "html.parser")

    healthcenters = soup.find(id="vaccsites")
    sites.extend(extract_healthcenters(healthcenters))

    pharmacies = soup.find(id="vaccsites2")
    sites.append(extract_pharmacies(pharmacies))

    mobile = soup.find(id="vaccsites3")
    sites.append(extract_pharmacies(mobile))

    with open(output_file, "w") as fout:
        for site in sites:
            json.dump(site, fout)
            fout.write("\n")
