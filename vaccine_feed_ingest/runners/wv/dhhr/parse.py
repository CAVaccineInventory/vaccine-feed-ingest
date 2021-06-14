#!/usr/bin/env python3

import collections
import datetime
import json
import os
import pathlib
import re
import sys
from enum import Enum
from typing import Dict, List, NamedTuple, Optional, Text

from bs4 import BeautifulSoup, ResultSet

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

if len(sys.argv) < 3:
    logger.error("Must pass two arguments: output_dir, input_dir")
    sys.exit(1)

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)
if input_dir is None:
    logger.error("Must pass an input_dir as second argument")
    sys.exit(1)
input_filepaths = input_dir.glob("*.html")
metadata_filepath = input_dir.joinpath("metadata.ndjson")

# Regex matching county names.
# Anchors are necessary because some sites have "County" in their name,
# and we don't want to parse those as counties.
_COUNTY_REGEX = re.compile(r"^.* Count(y|ies)$")
# Regex matching opening hours.
# hh:mm a.m. - hh:mm p.m.( and hh:mm a.m. - hh:mm p.m.)?
_OPENING_HOURS_REGEX = re.compile(
    r"\s*(?P<opens>\d+:\d+ [ap]\.?m\.?)\s*[-–]\s*(?P<closes>\d+:\d+ [ap]\.?m\.?)\s*(and (?P<opens2>\d+:\d+ [ap]\.?m\.?)\s*[-–]\s*(?P<closes2>\d+:\d+ [ap]\.?m\.?))?",
    re.IGNORECASE,
)
# Regex matching vaccine site info.
# Name, Street1, (Street2,)? City,? WV, zip.
# For simplicity, this accepts any string of details before the state and zip.
# The parser will split these up into individual details.
_SITE_REGEX = re.compile(
    r"\s*(?P<details>.+)\s*WV\s+(?P<zip>\d+)",
    re.IGNORECASE,
)


class DateFormat(Enum):
    """Expected date formats within the WV DHHR web page."""

    PAGE_UPDATED_DATE = "%m/%d/%Y"  # e.g. 06/07/2021
    SITE_OPENING_DATE = "%A, %B %d, %Y"  # e.g. Monday, June 7, 2021


def _parse_date(line: Text, format: DateFormat) -> Optional[Text]:
    """Parses the given line of text as a date string of the given format, if possible.
    Returns the date as an ISO 8601 date string YYYY-MM-DD.
    """
    line = line.strip()
    if not line:
        return None
    try:
        return datetime.datetime.strptime(line, format.value).date().isoformat()
    except ValueError:
        # Couldn't parse as date; carry on.
        return None


def _parse_first_line_as_date(paragraphs: ResultSet) -> Optional[Text]:
    """Parses the first line of `paragraphs` as a date string, if possible.
    Returns the date as an ISO 8601 date string YYYY-MM-DD.
    """
    logger.debug("Parsing a date from the first line")
    first_line = paragraphs[0].string if paragraphs else None
    if first_line:
        result = _parse_date(first_line, DateFormat.PAGE_UPDATED_DATE)
        logger.debug("Parsed date %s", result)
        return result
    logger.warning("Couldn't parse first line as a date: %s", first_line)
    return None


def _is_county(line: Text) -> bool:
    """Whether the given line holds a county name."""
    return _COUNTY_REGEX.match(line) is not None


class ParsedSite(NamedTuple):
    """Name and address of a vaccine site."""

    name: Optional[Text]
    street1: Optional[Text]
    street2: Optional[Text]
    city: Optional[Text]
    county: Optional[Text]
    state: Text
    zip: Text


class OpeningHours(NamedTuple):
    opens: Text
    closes: Text


# Type alias for a mapping from opening dates -> list of opening hours for each date.
OpeningTimes = Dict[Optional[Text], List[OpeningHours]]
# Type alias for parsed vaccine sites:
# site details -> opening dates -> list of opening hours for each date.
ParsedSites = Dict[ParsedSite, OpeningTimes]


def _parse_site(line: Text, county: Optional[Text]) -> Optional[ParsedSite]:
    """Parses the given `line`, denoting a vaccine site and its opening hours for a single day."""
    match = _SITE_REGEX.match(line)
    if match is None:
        return None
    # We need to handle both (name, street1, city) and (name, street1, street2, city).
    # For simplicity, the regex matches the entire middle section as `details`,
    # and we separate the components here by splitting on commas and trimming whitespace.
    split_details = [d.strip() for d in match.group("details").strip().split(",") if d]
    logger.debug("Comma-separated details: %s", split_details)
    name, street1, street2, city = [None] * 4
    num_details = len(split_details)
    if num_details == 3:
        name, street1, city = split_details
    elif num_details == 4:
        name, street1, street2, city = split_details
    elif num_details == 2:
        # Corner case caused by a typo: . used in the address instead of ,
        initial_details, city = split_details
        name, street1 = [d.strip() for d in initial_details.split(".") if d]
    else:
        logger.warn("Expected 2-4 comma-separated details, found %d", num_details)
    return ParsedSite(
        name=name,
        street1=street1,
        street2=street2,
        city=city,
        county=county,
        state="WV",
        zip=match.group("zip"),
    )


def _parse_opening_hours(line: Text) -> Optional[List[OpeningHours]]:
    """Parses the given `line`, denoting a vaccine site's opening hours for a single day."""
    match = _OPENING_HOURS_REGEX.match(line)
    if match is None:
        return None
    opening_hours = [OpeningHours(match.group("opens"), match.group("closes"))]
    # Some sites have two pairs of opening hours.
    # Could be generalised in future to any number of sets.
    if match.group("opens2") and match.group("closes2"):
        opening_hours.append(
            OpeningHours(match.group("opens2"), match.group("closes2"))
        )
    return opening_hours


def _write(
    output_path: pathlib.Path,
    sites: ParsedSites,
    date_updated: Optional[Text],
    fetched_from_uri: Optional[Text],
) -> None:
    """Writes the given parsed data to `output_path` as NDJSON."""
    logger.debug("Writing NDJSON to %s", output_path)
    with output_path.open("w") as output_file:
        for site, times in sites.items():
            obj = site._asdict()
            # Normalisation will need to convert this into days and hours.
            obj["opening_times"] = times
            obj["fetched_from_uri"] = fetched_from_uri
            obj["published_at"] = date_updated
            json.dump(obj, output_file)
            output_file.write("\n")


def _read_fetch_metadata(metadata_filepath: pathlib.Path) -> Optional[Text]:
    """Reads fetch metadata JSON from the given path."""
    logger.info("Reading fetch metadata NDJSON from: %s", metadata_filepath)
    try:
        with open(metadata_filepath, "r") as metadata_file:
            line = next(metadata_file, None)
            if line:
                metadata = json.loads(line)
                if metadata:
                    return metadata.get("fetched_from_uri", None)
    except FileNotFoundError as e:
        logger.warning(
            "Could not find fetch metadata file %s", metadata_filepath, exc_info=e
        )
    return None


def _parse(paragraphs: ResultSet) -> ParsedSites:
    """Parses the given lines as vaccine site information.
    Returns a mapping of the form:
    site details -> opening dates -> list of opening hours for each date.
    """
    # The same site will appear multiple times, each time for a different date
    # with potentially different opening hours. Collect them in a mapping.
    sites_to_times: ParsedSites = collections.defaultdict(
        lambda: collections.defaultdict(list)
    )

    # Expected sequence:
    # Monday, May 24, 2021
    # Abc County (may have multiple counties and sites for the same date)
    # 8:00 a.m. – 8:00 p.m.
    # Name, 100 Street Address, City, WV zip.
    # (may have multiple sites for the same date and county)
    opening_date: Optional[Text] = None
    opening_hours: List[OpeningHours] = []
    county: Optional[Text] = None
    logger.debug("Parsing HTML text")
    for p in paragraphs:
        # Multiple details may be within the same paragraph,
        # separated only by line breaks. Iterate through these.

        # Note: this is a little fragile to changes in the HTML markup.
        # To handle older versions of the page with details in separate <p> tags,
        # and inconsistent formatting tags within the text,
        # use `line = "".join(p.strings)`.
        for line in p.strings:
            # Trim whitespace and commas.
            line = line.strip().strip(",")
            line = line.replace("\n", " ")  # TODO do we need this?
            if not line:
                # Ignore effectively empty lines.
                continue
            # Try to parse as a county name.
            if _is_county(line):
                county = line
                logger.debug("Parsed county: %s", county)
            # Try to parse as a date.
            elif (
                candidate_date := _parse_date(line, DateFormat.SITE_OPENING_DATE)
            ) is not None:
                logger.debug("Parsed date: %s", candidate_date)
                opening_date = candidate_date
            # Try to parse as the opening hours of a vaccine site.
            elif (hours := _parse_opening_hours(line)) is not None:
                opening_hours = hours
                logger.debug("%s", opening_hours)
            # Try to parse as the name and address of a vaccine site.
            elif (parsed_site := _parse_site(line, county)) is not None:
                logger.debug("%s", parsed_site)
                sites_to_times[parsed_site][opening_date].extend(opening_hours)
                # Reset saved opening hours so we can process the next site.
                opening_hours = []
            else:
                logger.warning("Could not parse: '%s'", line)
    return sites_to_times


def main():
    fetched_from_uri = _read_fetch_metadata(metadata_filepath)

    if not input_filepaths:
        logger.warning("No input files found in %s", input_dir)

    for input_filepath in input_filepaths:
        filename = input_filepath.name.split(".", maxsplit=1)[0]
        output_filepath = output_dir / f"{filename}.parsed.ndjson"

        # Create the output directory if necessary.
        os.makedirs(output_dir, exist_ok=True)

        logger.info(
            "Parsing HTML to NDJSON: %s => %s",
            input_filepath,
            output_filepath,
        )

        logger.debug("Reading HTML from %s", input_filepath)
        with open(input_filepath, "r") as input_file:
            soup = BeautifulSoup(input_file, "html.parser")

        logger.info("HTML title: %s", soup.title.string)
        # The relevant data is assumed to be in <p> tags.
        paragraphs = soup.find_all("p")
        date_updated = _parse_first_line_as_date(paragraphs)
        if date_updated:
            # We've parsed the first line, so remove it.
            paragraphs.pop(0)
        parsed_sites = _parse(paragraphs)

        _write(output_filepath, parsed_sites, date_updated, fetched_from_uri)
        logger.info("Parsing complete. Output written to %s", output_filepath)


if __name__ == "__main__":
    main()
