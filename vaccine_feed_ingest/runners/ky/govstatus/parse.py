#!/usr/bin/env python

import json
import pathlib
import re
import sys

from bs4 import BeautifulSoup

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


def parse_address(address_el):
    """Given a tag with an address with a line break, parse out the address parts"""
    text = address_el.text
    address_parts = [s for s in address_el.stripped_strings]
    if len(address_parts) != 2:
        logger.error(f"failed to parse address: {text}")
        return None
    pat = re.compile(r"\s*(?P<city>.*?)\s*,?\s*(?P<state>[A-Z]{2})\s+(?P<zip>[0-9]+)")
    match = pat.search(address_parts[1])
    if match is None:
        logger.error(f"failed to parse address: {text}")
        return None
    return {
        "street1": address_parts[0],
        "city": match.group("city"),
        "state": match.group("state"),
        "zip": match.group("zip"),
    }


def sanitize_lat_long(s: str) -> float:
    """Clean up an input to only keep numeric characters and minus (`-`) symbol"""
    return float(re.sub(r"[^0-9-\.,\+]", "", s))


def parse_county(county_el):
    """Parse a location div under #VaccineLocations"""
    data_id = county_el.attrs["data-id"]

    name_els = county_el.select("h5")
    name = None
    if len(name_els) == 0:
        logger.error("No 'h5' tag found for county with data-id: {data_id}")
    else:
        if len(name_els) > 1:
            logger.warn(
                "Multiple 'h5' tags found for county with data-id: {data_id}, using the first."
            )
        name = name_els[0].text

    result = {
        "id": data_id,
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

    address_els = county_el.select("address")
    if len(address_els) == 0:
        logger.error("No 'address' tag found for {data_id} {name}")
    else:
        if len(address_els) > 1:
            logger.warn(
                "Multiple 'address' tags found for {data_id} {name}, using the first."
            )
        address_el = address_els[0]
        result["address"] = parse_address(address_el)

    return result


def parse_locations(document):
    locations_els = document.select("#VaccineLocations")
    if len(locations_els) == 0:
        raise Exception("No tag found with id '#VaccineLocations'")
    elif len(locations_els) > 1:
        logger.warn("Multiple tags found with id '#VaccineLocations', using the first")
    locations_el = locations_els[0]
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
