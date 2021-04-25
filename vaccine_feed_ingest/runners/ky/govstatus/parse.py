#!/usr/bin/env python

import json
import logging
import pathlib
import re
import sys

from bs4 import BeautifulSoup

logger = logging.getLogger("ky/govstatus/parse.py")


def parse_address(address_el):
    """Given a tag with an address with a line break, parse out the address parts"""
    text = address_el.text
    address_parts = [s for s in address_el.stripped_strings]
    if len(address_parts) != 2:
        logger.warn(f"failed to parse address: {text}")
        return None
    pat = re.compile(r"\s*(?P<city>.*?)\s*,?\s*(?P<state>[A-Z]{2})\s+(?P<zip>[0-9]+)")
    match = pat.search(address_parts[1])
    if match is None:
        logger.warn(f"failed to parse address: {text}")
        return None
    return {
        "street1": address_parts[0],
        "city": match.group("city"),
        "state": match.group("state"),
        "zip": match.group("zip"),
    }


def sanitize_lat_long(s: str) -> int:
    """Clean up an input to only keep numeric characters and minus (`-`) symbol"""
    return int(re.sub(r"[^0-9-]", "", s))


def parse_county(county_el):
    """Parse a location div under #VaccineLocations"""
    name = county_el.select("button h5")[0].text
    result = {
        "id": county_el.attrs["data-id"],
        "name": name,
        "county": county_el.attrs["data-county"],
        "lat": sanitize_lat_long(county_el.attrs["data-lat"]),
        "long": sanitize_lat_long(county_el.attrs["data-long"]),
    }
    action_online_el = county_el.find("a", text="Register Online")
    if action_online_el is not None:
        result["register_online_url"] = action_online_el.attrs["href"]

    action_phone_el = county_el.find("a", text="Register by Phone")
    if action_phone_el is not None:
        # tel:###-###-####
        result["register_phone"] = action_phone_el.attrs["href"]

    address_el = county_el.select("address")[0]
    if address_el is not None:
        result["address"] = parse_address(address_el)
    else:
        logger.warn(f"No address found for {name}")
    return result


def parse_locations(document):
    locations_el = document.select("#VaccineLocations")[0]
    locations = [
        parse_county(county_el)
        for county_el in locations_el.find_all(attrs={"data-county": True})
    ]
    return locations


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    output_dir = pathlib.Path(argv[0])
    input_dir = pathlib.Path(argv[1])

    input_path = input_dir / "govstatus.html"
    with input_path.open() as f:
        doc = BeautifulSoup(f, "html.parser")
        locations = parse_locations(doc)

    out_filepath = output_dir / "govstatus.parsed.ndjson"
    with out_filepath.open("w") as f_out:
        for row in locations:
            json.dump(row, f_out)
            f_out.write("\n")


if __name__ == "__main__":
    sys.exit(main())
