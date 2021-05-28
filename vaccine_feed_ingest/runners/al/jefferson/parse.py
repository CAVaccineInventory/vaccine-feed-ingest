#!/usr/bin/env python

import datetime
import json
import logging
import os
import pathlib
import re
import subprocess
import sys
from typing import List, NamedTuple, Optional, Text, Tuple

import pytz
from lxml import etree

# Configure logger. Increase to DEBUG for verbose logging in this parser.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("al/jefferson/parse.py")

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

# Jefferson vaccine sites are listed in a PDF
input_filepaths = input_dir.glob("*.pdf")

metadata_filepath = input_dir.joinpath("metadata.ndjson")


class Region(NamedTuple):
    """A rectangular region in a document.
    `(x0,y0)` are the top left coordinates and
    `(x1,y1)` are the bottom right coordinates.
    """

    x0: int
    y0: int
    x1: int
    y1: int


class ParsedSite(List[Text]):
    """Parsed content likely to be from a single vaccine site in Jefferson County.

    This is a list of lines of text.
    """


class ParsedProvider(NamedTuple):
    """Parsed content for a single vaccine provider in Jefferson County.

    This consists of an index `number` on the page,
    a list of provider sites,
    optionally a hyperlink that was detected within the text, and
    optionally an updated-at `date` that was detected within the text.

    In the PDF there is usually an image containing
    the provider name, but this is not parsed.
    """

    number: int
    sites: List[ParsedSite]
    link: Optional[Text]
    date: Optional[Text]


class ParsedPage(NamedTuple):
    """Parsed content from a single PDF page of vaccine provider info
    in Jefferson County.

    This consists of a page `number` and a list of parsed vaccine `providers`.
    """

    number: int
    providers: List[ParsedProvider]


def _get_region(e: etree.Element) -> Region:
    """Converts the `left`, `top`, `width`, and `height` attributes
    of the given XML element into a rectangular coordinate region."""
    x0 = int(e.attrib["left"])
    x1 = x0 + int(e.attrib.get("width", 0))
    y0 = int(e.attrib["top"])
    y1 = y0 + int(e.attrib.get("height", 0))
    return Region(x0, y0, x1, y1)


def _get_primary_link_tag(e: etree.Element) -> Optional[etree.Element]:
    """
    Gets the first significant `<a>` tag under `e`, if any.

    The heuristic used looks for `<a>` tags with
    at least 4 characters of link text.
    This heuristic helps filter out two unusual cases in the PDF/XML:
    - blank links
    - overlapping link regions from a different column:
    `<text><a href="wronglink">w</a><a href="rightlink">ww.provider.com</a></text>`
    """
    for link_tag in e.findall("a"):
        if sum(map(len, link_tag.itertext())) >= 4:
            return link_tag
    return None


def _parse_provider(number: int, text_tags: List[etree.Element]) -> ParsedProvider:
    """Parses information about a possible vaccine provider
    from XML obtained from the Jefferson County PDF.
    """
    text = []
    link: Optional[Text] = None
    for text_tag in text_tags:
        link_tag = _get_primary_link_tag(text_tag)
        if link_tag is not None and link is None:
            link = link_tag.attrib["href"]
            logger.info("Found link %s for provider %d", link, number)
        elif link_tag is not None:  # link is not None
            candidate_link = link_tag.attrib["href"]
            # Ignore this link, because we already have one for the provider.
            # TODO should we save one link per site, not per provider?
            if candidate_link != link:
                logger.info(
                    "Already have link %s for provider %d; ignoring link %s",
                    link,
                    number,
                    candidate_link,
                )
        else:
            # This line is not primarily a hyperlink.
            # Record non-link text, trimming whitespace.
            text.append("".join(map(lambda t: t.strip(), text_tag.itertext())))

    logger.info("Grouping site details for provider %d", number)
    sites, date = _parse_sites(text)
    return ParsedProvider(number=number, sites=sites, link=link, date=date)


def _group_text_by_provider(page: etree.Element) -> List[List[etree.Element]]:
    """
    Groups the `text` tags on the given XML page into a list of lists.
    Each sublist contains the `text` tags for a single vaccine provider.

    The Jefferson County document has header images for each provider,
    which introduce relatively large gaps between lines of text
    for different providers.
    This function uses heuristics to detect these gaps between lines,
    and hence to decide which lines are for the same provider.
    """
    lines: List[etree.Element] = page.findall("text")
    if not lines:
        return []

    # Process the first line on the page, and initialise
    # the variables used to track the current provider.
    first_line = lines[0]
    first_text_region = _get_region(first_line)
    logger.debug("Line 0: %s", first_text_region)
    # The lines of text for the current provider.
    current_provider: List[Text] = [first_line]
    # The coordinates of the last line in the current provider.
    current_provider_last_line = first_text_region
    # The delta in y-coordinates between the last two lines in the current provider.
    current_provider_last_y_delta: Optional[int] = None
    # All providers seen so far, including the current provider.
    providers = [current_provider]

    # Process all remaining lines on the page.
    for i, line in enumerate(lines[1:]):
        text_region = _get_region(line)

        # Compute the difference in coordinates from the previous line.
        x_delta = text_region.x0 - current_provider_last_line.x0
        y_delta = text_region.y0 - current_provider_last_line.y1

        # Use heuristics to decide whether to consider this line of text
        # as part of the current provider, or the start of a new provider.
        # Assume different providers are relatively far apart on the page.
        MAX_ABSOLUTE_CHANGE = 200  # pixels
        MAX_LINE_SPACING_CHANGE = 100  # pixels
        if current_provider_last_y_delta is None:
            # The previous line was the first line of a provider.
            # Check that the x and y coordinates haven't changed much between the two lines.
            use_current_provider = (
                abs(x_delta) < MAX_ABSOLUTE_CHANGE
                and abs(y_delta) < MAX_ABSOLUTE_CHANGE
            )
        else:
            # The previous line was line 2+ of a provider,
            # so we know the delta between the last two lines of the provider.
            # Check that the x coordinate hasn't changed too much between this line and the previous line,
            # and that the difference in y coordinates between this line and the previous line
            # is about the same as the difference in y coordinates between the previous 2 lines.
            use_current_provider = (
                abs(x_delta) < MAX_ABSOLUTE_CHANGE
                and abs(y_delta - current_provider_last_y_delta)
                < MAX_LINE_SPACING_CHANGE
            )

        if use_current_provider:
            # Add to the current provider.
            current_provider.append(line)
            current_provider_last_line = text_region
            current_provider_last_y_delta = y_delta
        else:
            # Start a new provider.
            current_provider = [line]
            providers.append(current_provider)
            current_provider_last_line = text_region
            current_provider_last_y_delta = None
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Line %d: %s, x_delta=%s, y_delta=%s, use_current_provider=%s, content=%s",
                i,
                text_region,
                x_delta,
                y_delta,
                use_current_provider,
                etree.tostring(line, method="text", encoding="utf-8"),
            )

    return [p for p in providers if p]


def _parse_page(page: etree.Element) -> ParsedPage:
    """Parses a 'page' tag of the Jefferson County XML data
    into the list of vaccine providers described on the page."""
    page_num = int(page.attrib.get("number", 0))
    logger.info("Processing page %s", page_num)

    # Group text by likely providers.
    text_by_provider = _group_text_by_provider(page)

    parsed_providers: List[ParsedProvider] = []
    # Parse each group of text. Python 3.6+ preserves insertion order in dicts.
    for i, text_tags in enumerate(text_by_provider):
        provider = _parse_provider(i, text_tags)
        parsed_providers.append(provider)

        # Debug logging for the grouping of lines into providers,
        # skipped in production runs.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Provider: page %s, index %d, %d text tags, %d sites, link %s",
                page_num,
                i,
                len(text_tags),
                len(provider.sites),
                provider.link,
            )
            logger.debug("Raw text tags:")
            for text_tag in text_tags:
                logger.debug(
                    "\t%s", etree.tostring(text_tag, method="text", encoding="utf-8")
                )
            logger.debug("Parsed text lines:")
            for site in provider.sites:
                logger.debug("Site with %d lines", len(site))
                for line in site:
                    logger.debug("\t%s", line)
    return ParsedPage(number=page_num, providers=parsed_providers)


_GENERIC_ENTRY_PREFIXES = [
    "AL \\d+/\\d+",
    "CONDADO DE JEFFERSON",
    "COVID-19",
    "If you have questions",
    "JEFFERSON COUNTY",
    "PLEASE",
    "PREGUNTAS",
    "SITIOS DE",
    "This is a list of agencies",
    "UPDATED",
    "VACCINE FAQ",
]
_GENERIC_ENTRY_REGEX = re.compile("(" + "|".join(_GENERIC_ENTRY_PREFIXES) + ")")
_UPDATED_DATE_REGEX = re.compile(r"UPDATED ([a-z]+\s+\d+, \d+)", re.IGNORECASE)


def _is_valid_site(site: ParsedSite) -> bool:
    """Filters out entries in the Jefferson County document
    that are unlikely to contain vaccine provider site information,
    based on the contents of their first line."""
    return len(site) > 0 and re.match(_GENERIC_ENTRY_REGEX, site[0]) is None


def _get_updated_date(lines: List[Text]) -> Optional[Text]:
    """If the first entry of `lines` contains a date stamp of the form
    UPDATED <MONTH> dd, YYYY, then returns that date, localized to US/Central
    for Alabama, and then converted to an ISO 8601 date string.
    Otherwise returns None.
    """
    if lines:
        match = re.match(_UPDATED_DATE_REGEX, lines[0])
        if match is not None:
            logger.info("Found updated date %s", lines[0])
            # e.g. May 25, 2021
            eastern = pytz.timezone("US/Central")
            updated_date = eastern.localize(
                datetime.datetime.strptime(match.group(1), "%B %d, %Y")
            ).isoformat()
            return updated_date
    return None


def _parse_sites(
    lines: List[Text],
) -> Tuple[List[ParsedSite], Optional[Text]]:
    """Gets a pair (sites, date) where
    `sites` is a list of vaccine site details for the given vaccine provider,
    and `date` is an updated-at date detected in the text.

    Each entry in `sites` is a list of lines of text.
    Each entry is intended to describe a single vaccine site,
    but there are some exceptions where a single entry will contain
    multiple addresses for different sites of the same provider.
    """
    sites = []
    current_site: ParsedSite = ParsedSite()
    sites.append(current_site)
    # Assume a blank line is used to separate different sites
    # for the same provider.
    # This assumption holds for most but not all providers.
    # When false, it will lead to a single site being reported
    # for the provider, with all the details joined together by newlines.
    for line in lines:
        if line:
            current_site.append(line)
        else:
            current_site = ParsedSite()
            sites.append(current_site)

    date = None
    result = []
    for site in sites:
        date = date or _get_updated_date(site)
        if _is_valid_site(site):
            result.append(site)
        else:
            logger.debug("Not valid site: %s", site)
    return result, date


def main():
    for input_filepath in input_filepaths:
        filename = input_filepath.name.split(".", maxsplit=1)[0]
        xml_filepath = output_dir / f"{filename}.xml"
        output_filepath = output_dir / f"{filename}.parsed.ndjson"

        # Create the output directory if necessary.
        os.makedirs(output_dir, exist_ok=True)

        logger.info(
            "Parsing PDF to XML: %s => %s",
            input_filepath,
            xml_filepath,
        )

        # Run the pdftohtml executable to convert PDF to XML.
        try:
            subprocess.check_call(
                [
                    "pdftohtml",
                    # -c -hidden to handle complex structure
                    "-c",
                    "-hidden",
                    # ignore images
                    "-i",
                    "-nomerge",
                    "-xml",
                    input_filepath,
                    xml_filepath,
                ]
            )
        except FileNotFoundError as e:
            logger.error(
                "Could not find pdftohtml executable; check it is installed and on the PATH"
            )
            raise e

        # Read the metadata JSON produced by the fetch step.
        logger.info("Reading fetch metadata NDJSON from: %s", metadata_filepath)
        fetched_from_uri: Optional[Text] = None
        with open(metadata_filepath, "r") as metadata_file:
            line = next(metadata_file, None)
            if line:
                metadata = json.loads(line)
                if metadata:
                    fetched_from_uri = metadata.get("fetched_from_uri", None)

        logger.info(
            "Parsing XML to NDJSON: %s => %s",
            xml_filepath,
            output_filepath,
        )

        # Parse the XML into memory.
        with open(xml_filepath, "rb") as xml_file:
            # Parser options for security purposes:
            # - resolve_entities=False to disable XML external entity expansion
            # - huge_tree=False (default) to mitigate against XML exponential entity expansion
            # Despite these security mitigations, we are assuming here that pdftohtml
            # is unlikely to create dangerous XML.
            # Use with caution when the input XML files are untrusted.
            parser = etree.XMLParser(resolve_entities=False, huge_tree=False)
            tree = etree.parse(xml_file, parser)

        # Convert the XML into our own representation.
        root = tree.getroot()
        parsed_pages: List[ParsedPage] = []
        for page_tag in root.findall("page"):
            parsed_page = _parse_page(page_tag)
            parsed_pages.append(parsed_page)

        dates = (provider.date for page in parsed_pages for provider in page.providers)
        date = next(filter(None, dates), None)

        # Write output file.
        with output_filepath.open("w") as fout:
            for page in parsed_pages:
                for provider in page.providers:
                    for site in provider.sites:
                        # Known limitations:
                        # - The provider names, e.g. Walgreens, are in header images
                        #   so this code can't read them.
                        # - Some providers have no blank lines separating their different sites,
                        #   so they appear as a single site with multiple addresses in the details.
                        #   This will need to be addressed during normalisation.
                        json.dump(
                            {
                                "page": page.number,
                                "provider": provider.number,
                                "link": provider.link,
                                "details": site,
                                "fetched_from_uri": fetched_from_uri,
                                "published_at": date,
                            },
                            fout,
                        )
                        fout.write("\n")
        logger.info("Parsing complete. Output written to %s", output_filepath)


if __name__ == "__main__":
    main()
