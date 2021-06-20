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
from bs4.element import Tag

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


# Regex matching county names.
# Some sites have "County" in their name,
# and we don't want to parse those as counties.
_COUNTY_REGEX = re.compile(r"^.* Count(y|ies)")


def _is_county(line: Text) -> bool:
    """Whether the given line holds a county name."""
    return _COUNTY_REGEX.match(line) is not None


def _named_group(label: Text, regex: Text) -> Text:
    """Returns the regex string `(?P<label>regex)`
    This matches `regex` as a capture group named `label`."""
    return r"(?P<" + label + r">" + regex + r")"


# Regex matching state and zip, e.g. WV 25504.
_STATE_ZIP_REGEX = re.compile(
    r"WV[\s,]\s*" + _named_group("zip", r"\d+"),
    re.IGNORECASE,
)


def _parse_zip(text: Text) -> Optional[Text]:
    """If `text` contains a state and zip, e.g. `WV 25504`, returns the zip,
    or `None` otherwise."""
    match = _STATE_ZIP_REGEX.match(text)
    return None if match is None else match.group("zip")


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


class ParsedSiteWithHours(NamedTuple):
    """Vaccine site details, with opening hours for a single date.
    The result of parsing a single line of fetched site data."""

    site: ParsedSite
    hours: List[OpeningHours]


def _get_hours_regex(group_label: Text) -> Text:
    # hh:mm a.m. or hh:mm p.m.
    return _named_group(group_label, r"\d+:\d+ [ap]\.?m\.?")


# Opening hours
# hh:mm a.m. - hh:mm p.m.( and hh:mm a.m. - hh:mm p.m.)?
_OPENING_HOURS_REGEX_STRING = r"\s*".join(
    [
        _get_hours_regex("opens"),
        r"[-–]",
        _get_hours_regex("closes"),
        r"(and",
        _get_hours_regex("opens2"),
        r"[-–]",
        _get_hours_regex("closes2"),
        r")?",
    ]
)
_OPENING_HOURS_REGEX = re.compile(
    _OPENING_HOURS_REGEX_STRING,
    re.IGNORECASE,
)


def _parse_opening_hours(text: Text) -> Optional[List[OpeningHours]]:
    match = _OPENING_HOURS_REGEX.match(text)
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


def _parse_site(line: Text, county: Optional[Text]) -> Optional[ParsedSiteWithHours]:
    """Parses the given `line`, denoting a vaccine site and its opening hours for a single day."""

    # There is a variable number of details, e.g. street addresses can have 1 or 2 parts.
    # Split the string on commas to obtain individual details, trim whitespace,
    # and then use regex matching on some of the individual details.
    details = [d.strip() for d in line.strip().split(",") if d]
    if not details:
        return None
    logger.debug("Comma-separated details: %s", details)

    # Parse first detail as opening hours.
    opening_hours = _parse_opening_hours(details.pop(0))
    if opening_hours is None:
        return None

    # Parse last detail as state and zip, e.g. WV 25504.
    if not details:
        return None
    zip = _parse_zip(details.pop())
    if zip is None:
        return None

    # We need to handle both (name, street1, city) and (name, street1, street2, city).
    name, street1, street2, city = [None] * 4
    num_details = len(details)
    if num_details == 3:
        name, street1, city = details
    elif num_details == 4:
        name, street1, street2, city = details
    elif num_details == 2:
        # Corner case caused by a typo: . used in the address instead of ,
        initial_details, city = details
        name, street1 = [d.strip() for d in initial_details.split(".") if d][0:2]
    else:
        logger.warning(
            "Expected 2-4 remaining details, found %d: %s", num_details, details
        )
    site = ParsedSite(
        name=name,
        street1=street1,
        street2=street2,
        city=city,
        county=county,
        state="WV",
        zip=zip,
    )
    return ParsedSiteWithHours(site=site, hours=opening_hours)


# Type alias for a mapping from opening dates -> list of opening hours for each date.
OpeningTimes = Dict[Optional[Text], List[OpeningHours]]
# Type alias for parsed vaccine sites:
# site details -> opening dates -> list of opening hours for each date.
ParsedSites = Dict[ParsedSite, OpeningTimes]


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


def _get_lines(paragraph: Tag) -> List[Text]:
    """Gets a list of non-trivial trimmed lines of text in the given `<p>` tag.

    HTML lines are considered separate if there are `<br>` tags in between.
    Other formatting remains within a single line.

    This function attempts to be robust to inline formatting in the HTML markup,
    concatenating their text contents to produce each line.
    """
    # Replace <br> tags with a fixed delimiter, so we can split on them.
    # Source: https://stackoverflow.com/questions/61421079/beautifulsoup-get-text-ignoring-line-breaks-br
    _BR_DELIMITER = "#BR_DELIMITER#"
    for line_break in paragraph.find_all("br"):
        line_break.replace_with(_BR_DELIMITER)

    # Take all the strings in the paragraph and join them into one.
    # This is preferable to iterating over `p.strings` as separate lines,
    # because it handles text nested in tags, including inline formatting like <em> and <sup>.
    # For example, `<p>1<sup>st</sup></p>` should be "1st", not ["1", "st"].
    paragraph_text = "".join(paragraph.strings)
    # Split up lines separated by <br> tags.
    # These are sometimes used to separate county names from vaccine site details.
    lines = paragraph_text.split(_BR_DELIMITER)
    # Replace \n with spaces.
    # We need this because some parsed HTML lines contain \n characters,
    # but these are not semantically meaningful, unlike <p> or <br> tags.
    # Trim whitespace and commas.
    lines = [line.replace("\n", " ").strip().strip(",") for line in lines]
    # Ignore effectively empty lines.
    return list(filter(None, lines))


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
    # 8:00 a.m. – 8:00 p.m., Name, 100 Street Address, City, WV zip.
    # (may have multiple sites for the same date and county)
    opening_date: Optional[Text] = None
    county: Optional[Text] = None
    logger.debug("Parsing HTML text")
    for p in paragraphs:
        # Multiple details may be within the same paragraph,
        # separated only by line breaks. Iterate through these.
        for line in _get_lines(p):
            logger.debug("Parsing line: %s", line)
            # Try to parse as a date.
            if (
                candidate_date := _parse_date(line, DateFormat.SITE_OPENING_DATE)
            ) is not None:
                logger.debug("Parsed date: %s", candidate_date)
                opening_date = candidate_date
            # Try to parse as the opening hours, name, and address of a vaccine site.
            elif (parsed := _parse_site(line, county)) is not None:
                logger.debug("%s", parsed.site)
                logger.debug("Parsed hours for date %s: %s", opening_date, parsed.hours)
                sites_to_times[parsed.site][opening_date].extend(parsed.hours)
            # Try to parse as a county name.
            # This is deliberately done after attempting to parse as a full vaccine site:
            # some sites have the county in their name and would otherwise get parsed as a county.
            elif _is_county(line):
                county = line
                logger.debug("Parsed county: %s", county)
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
