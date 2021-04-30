#!/usr/bin/env python

import json
import logging
import pathlib
import sys

from lxml import etree
from pykml import parser

logger = logging.getLogger("co/colorado_gov/parse.py")


def parse_point(element):
    coords = element.coordinates.text.strip().split(",")
    return {"long": coords[1], "lat": coords[0]}


def parse_extended_data(element):
    result = {}
    for data in element.Data:
        result[data.attrib["name"]] = data.value.text
    return result


element_parsers = {
    "Point": parse_point,
    "ExtendedData": parse_extended_data,
}


def parse(input_file):
    with input_file.open("r") as f:
        contents = f.read().encode("UTF-8")
        root = parser.fromstring(contents)

    locations = []
    for folder in root.Document.Folder:
        for place in folder.Placemark:
            location = {"_folder_name": folder.name.text}
            for item in place.getchildren():
                key = etree.QName(item).localname
                if key in element_parsers:
                    location[key] = element_parsers[key](item)
                else:
                    # if key starts with an uppercase letter, there's probably
                    # a element to be parsed more that just '.text'
                    if key[0].isupper():
                        logger.warn(f"WARNING: {key} should probably have a parser")
                    location[key] = item.text
            if "description" in location:
                location["description"] = [
                    x.strip() for x in location["description"].split("<br>")
                ]
            locations.append(location)

    return locations


def main(argv):
    output_dir = pathlib.Path(argv[0])
    input_dir = pathlib.Path(argv[1])

    input_file = input_dir / "colorado_gov.kml"

    locations = parse(input_file)

    out_filepath = output_dir / "locations.parsed.ndjson"
    with out_filepath.open("w") as f:
        for obj in locations:
            json.dump(obj, f)
            f.write("\n")


# If this file is being run from the CLI instead of imported as a module
if __name__ == "__main__":
    # discard the first item in sys.argv as it's the script name.
    # Example: '.../fetch.py'
    argv = sys.argv[1:]

    sys.exit(main(argv))
