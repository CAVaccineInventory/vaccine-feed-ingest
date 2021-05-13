#!/usr/bin/env python

import json
import pathlib
import re
import sys


def parse_address(address):
    """given a string of an address formatted accross multiple lines, parse
    the parts of the address"""
    if address is None or address.strip() == "":
        return {}
    address_parts = re.split(r"\r?\n", address.strip())

    # These addresses don't have a consistent format so parsing is a bit rough
    # Not all addresses include a zip code
    # Addresses do not use a consistent pattern of commas to separate
    # city, state zip
    pattern = re.compile(r"(?P<rest>.*?)(\s+(?P<zip>[0-9]{5,}))?\s*$")
    match = pattern.search(address_parts[-1])
    if match is None:
        raise Exception(f"failed to parse zip code in address: {address}")

    rest = match.group("rest")
    tn_pattern = re.compile(r",?\s*\b(TN|Tennessee)\b", re.IGNORECASE)
    # if re.search(tn_pattern, rest) is None:
    #     raise Exception(f"This address doesn't look like it's for Tennessee: {address}")
    city = re.sub(tn_pattern, "", rest)

    return {
        "lines": address_parts[:-1],
        "city": city,
        "state": "TN",
        "zip": match.group("zip"),
    }


def parse_phone(phone_description):
    if phone_description is None or phone_description.strip() == "":
        return None
    pattern = re.compile(r"[^\d]*?(?P<number>[\d\s\(\)-]{10,}).*?")
    match = pattern.search(phone_description)
    if match is None:
        raise Exception(f"failed to parse phone: {phone_description}")
    return match.group("number").strip()


def parse_location(location):
    result = {
        "id": location["Id"],
        "title": location["Title"],
        "lat": location["Latitude"],
        "long": location["Longitude"],
    }
    if location["Description"] is not None:
        desc_parts = location["Description"].split("<br>")
        if len(desc_parts) >= 1:
            result["address"] = parse_address(desc_parts[0].strip())
        if len(desc_parts) >= 2:
            result["phone"] = parse_phone(desc_parts[1].strip())
    return result


def main(argv):
    output_dir = pathlib.Path(argv[0])
    input_dir = pathlib.Path(argv[1])

    input_file = input_dir / "locations.json"
    with input_file.open("r") as f:
        locations = json.load(f)

    parsed_locations = [parse_location(loc) for loc in locations]

    out_filepath = output_dir / "locations.parsed.ndjson"
    with out_filepath.open("w") as f:
        for obj in parsed_locations:
            json.dump(obj, f)
            f.write("\n")


# If this file is being run from the CLI instead of imported as a module
if __name__ == "__main__":
    # discard the first item in sys.argv as it's the script name.
    # Example: '.../fetch.py'
    argv = sys.argv[1:]

    sys.exit(main(argv))
