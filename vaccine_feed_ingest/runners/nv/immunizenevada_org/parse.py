#!/usr/bin/env python

import html.parser
import json
import pathlib
import re
import sys


class ImmunizeNVParser(html.parser.HTMLParser):
    """Parse the HTML snippet that contains the locator data."""

    def __init__(self):
        super().__init__()
        self.current_title = ""
        self.current_url = ""
        self.state = ""
        self.result = {}

    def handle_starttag(self, tag, attributes):
        attrs = {attr[0]: attr[1] for attr in attributes}

        if "class" in attrs:
            if "views-field-title" in attrs["class"].split():
                self.state = "views-field-title"
                return

            if "views-field-field-address" in attrs["class"].split():
                self.state = "views-field-field-address"
                return

            if "views-field-field-contact-phone" in attrs["class"].split():
                self.state = "views-field-field-contact-phone"
                return

        if tag == "div" and self.state == "views-field-field-address":
            self.state = "address-field-content"

        if tag == "div" and self.state == "views-field-field-contact-phone":
            self.state = "contact-phone-field-content"

        if tag == "a" and self.state == "views-field-title":
            self.state = "title-field-content"
            self.current_url = attrs["href"]

        if tag == "div" and "data-lat" in attrs:
            self.result[self.current_title]["lat"] = attrs["data-lat"]
            self.result[self.current_title]["lng"] = attrs["data-lng"]

    def handle_endtag(self, tag):
        self.state = ""

    def handle_data(self, data):
        if self.state == "title-field-content":
            self.current_title = data.strip()
            if self.current_title not in self.result:
                self.result[self.current_title] = {
                    "title": self.current_title,
                    "url": self.current_url,
                }
                self.current_url = ""

        if self.state == "address-field-content":
            self.result[self.current_title]["address"] = data.strip()

        if self.state == "contact-phone-field-content":
            self.result[self.current_title]["contact-phone"] = data.strip()


def extract_locator_data(json_data):
    """Extract the HTML snippet that contains the locator data."""

    result = ""
    objs = json.loads(json_data)
    for obj in objs:
        if obj["command"] == "insert" and obj["method"] == "replaceWith":
            result = obj["data"]
    return result


def generate_id(name):
    """Generate a stable ID for a location from the name.

    We don't want duplicate entries for the same location. If the
    same location is listed multiple times with minor differences
    in the name (extra space, or age info in parentheses), we want
    to produce a single, consistent ID.

    """
    # Strip off parenthetical age info found in some NV location names.
    #
    # This regex only works if the age restriction follows the exact format
    # where the numeric age is followed by a "+" and is contained in
    # parentheses.
    id = re.sub(r"\([0-9]+\+\)", "", name)

    # Strip whitespace from ends.
    id = id.strip()

    # Lower-case.
    id = id.lower()

    # Only keep alphanumeric characters, hyphens, and spaces.
    id = re.sub(r"[^a-z0-9 -]", "", id)

    # Replace interior whitespace with hyphens
    id = re.sub(r"\s+", "-", id)

    return id


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    output_dir = pathlib.Path(argv[0])
    input_dir = pathlib.Path(argv[1])
    json_filepaths = input_dir.glob("*.json")

    for input_file in json_filepaths:
        parser = ImmunizeNVParser()
        slug = input_file.name.split(".", maxsplit=1)[0]
        output_file = output_dir / f"{slug}.parsed.ndjson"

        with open(input_file, "r") as in_fh:
            content = in_fh.read()
            html_data = extract_locator_data(content)
            parser.feed(html_data)

        with open(output_file, "w") as out_fh:
            parsed = parser.result
            for k in sorted(parsed.keys()):
                parsed[k]["id"] = generate_id(parsed[k]["title"])
                line = json.dumps(parsed[k])
                out_fh.write(line)
                out_fh.write("\n")


if __name__ == "__main__":
    sys.exit(main())
