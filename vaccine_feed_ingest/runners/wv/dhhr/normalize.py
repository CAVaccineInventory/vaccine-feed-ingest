#!/usr/bin/env python

import json
import pathlib
import sys
import time
from datetime import datetime
from hashlib import md5
from typing import Dict, List, Optional, Text, Tuple

import pytz
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import normalize_url, normalize_zip

logger = getLogger(__file__)


_SOURCE_NAME = "wv_dhhr"


def _generate_id(name: Optional[Text], address: schema.Address) -> Text:
    """Generates a stable source ID for the given name and address data."""
    candidate_data: List[Optional[Text]] = [
        name,
        address.street1,
        address.street2,
        address.city,
        address.state,
        address.zip,
    ]
    return _md5_hash(candidate_data)


def _make_site_id(source: schema.Source) -> Text:
    """Returns a site ID compatible with `source`, according to the schema validation rules."""
    return f"{source.source}:{source.id}"


def _md5_hash(inputs: List[Optional[Text]]) -> Text:
    """Generates an md5 checksum from the truthy inputs."""
    return md5("".join(filter(None, inputs)).encode("utf-8")).hexdigest()


def _normalize_date(date: Optional[Text]) -> Optional[datetime]:
    """Gets the given `date` string as a `datetime` object
    in the US Eastern timezone, or `None` if the string cannot be converted."""
    if not date:
        return None
    eastern = pytz.timezone("US/Eastern")
    try:
        return eastern.localize(datetime.strptime(date, "%Y-%m-%d"))
    except ValueError as e:
        logger.warning("Could not parse date: %s", date, exc_info=e)
        return None


def _normalize_date_string(date: Optional[Text]) -> Optional[Text]:
    """Gets the given `date` string as a string in ISO-8601 format
    in the US Eastern timezone, or `None` if the string cannot be converted."""
    normalized_date = _normalize_date(date)
    return normalized_date.isoformat() if normalized_date else None


def _normalize_time(time_string: Optional[Text]) -> Optional[Text]:
    """Converts a time string of the form `hh:mm a.m.` or `hh:mm p.m.` (12-hour clock)
    to a time string of the form `hh:mm` (24-hour clock).
    Returns `None` if the given value cannot be parsed as such a time string."""
    if not time_string:
        return None
    # Remove dots, if any, and use uppercase AM/PM (expected by English locales).
    time_string = time_string.replace(".", "").upper()
    try:
        time_tuple = time.strptime(time_string, "%I:%M %p")
        return time.strftime("%H:%M", time_tuple)
    except ValueError:
        logger.warning("Couldn't parse time: %s", time_string)
        return None


def _make_opening_dates_contiguous(dates: List[datetime]) -> List[schema.OpenDate]:
    """
    Converts an list of `dates` into an equivalent list of inclusive `OpenDate` ranges.

    For example, `[2021-06-01, 2021-06-02, 2021-06-04]` becomes
    `[{opens: 2021-06-01, closes: 2021-06-02}, {opens: 2021-06-04, closes: 2021-06-04}]`.
    """
    dates.sort()
    ranges: List[schema.OpenDate] = []
    current_opens: Optional[datetime] = None
    current_closes: Optional[datetime] = None
    # Invariants:
    # - current_range.{opens,closes} are None at the beginning, and non-None otherwise.
    # - At iteration i (date = dates[i]):
    #   dates[0..i-1] = concat([r.opens..r.closes] for r in ranges) + [current_opens..current_closes]
    #   Here [ ] denote inclusive ranges on both ends.
    for date in dates:
        if not (current_opens and current_closes):
            # First date seen. Start with a singleton range.
            current_opens = date
            current_closes = date
        else:
            # Compute the difference in days between this new date and the end date of the current range.
            delta = (date - current_closes).days
            if delta == 0:
                # Same date seen twice in succession.
                # Unlikely, but handle it anyway by changing nothing.
                logger.debug("Encountered same date twice in succession: %s", date)
            elif delta == 1:
                # Extend the current range by one day to include this date.
                current_closes = date
            else:
                # There is a gap of at least 1 day between the current range and this date.
                # Save the current range.
                ranges.append(
                    schema.OpenDate(opens=current_opens, closes=current_closes)
                )
                # And start a new one.
                current_opens = date
                current_closes = date
    # Include the last range.
    ranges.append(schema.OpenDate(opens=current_opens, closes=current_closes))
    return ranges


def _normalize_opening_times(
    opening_times: Optional[Dict[Text, List[Tuple[Text, Text]]]],
) -> Optional[Tuple[List[schema.OpenDate], List[schema.OpenHour]]]:
    """Normalizes the given opening times for a single vaccine site.

    Expects opening times as a dict,
    where each key is a YYYY-MM-DD date string, and
    each value is a list of pairs of [open time, close time].

    Returns a pair of (opening_dates, opening_hours) suitable for the normalized schema,
    or None if the dates are invalid.
    """
    if not opening_times or not isinstance(opening_times, dict):
        return None
    # The normalized schema associates opening hours with a day of the week, not a specific date.
    # ASSUMPTION: the parsed data won't contain, for example,
    # different opening hours for the same site on two different Mondays.
    # The DHHR data is produced for one week at a time, so is expected to satisfy this assumption.
    # If this assumption goes wrong, we'll have multiple conflicting entries for opening hours on the same day.
    opening_dates: List[datetime] = []
    opening_hours: List[schema.OpenHour] = []
    for date_string, time_windows in opening_times.items():
        normalized_date = _normalize_date(date_string)
        if normalized_date is None:
            # Can't do much with an invalid date, and we've logged it already.
            continue
        opening_dates.append(normalized_date)
        day_lowercase = normalized_date.strftime("%A").lower()
        for time_window in time_windows:
            # Can have multiple opening windows for a single day.
            # This assumes that downstream processing can handle
            # multiple OpenHour objects with the same day of the week
            # but different open/close times.
            open_time, close_time = map(_normalize_time, time_window)
            if open_time and close_time:
                opening_hours.append(
                    schema.OpenHour(
                        day=day_lowercase, opens=open_time, closes=close_time
                    )
                )
            else:
                # Shouldn't happen, log and carry on.
                logger.warning(
                    "Invalid time window %s, normalized as %s",
                    time_window,
                    [open_time, close_time],
                )
    return _make_opening_dates_contiguous(opening_dates), opening_hours


def normalize(site: dict) -> schema.NormalizedLocation:
    """Converts the parsed `site` into a normalized site location."""
    name = site.get("name")
    address = schema.Address(
        street1=site.get("street1"),
        street2=site.get("street2"),
        city=site.get("city"),
        state=site.get("state"),
        zip=normalize_zip(site.get("zip")),
    )
    source = schema.Source(
        source=_SOURCE_NAME,
        id=_generate_id(name, address),
        fetched_from_uri=normalize_url(site.get("fetched_from_uri")),
        published_at=_normalize_date_string(site.get("published_at")),
        data=site,
    )
    county = site.get("county")
    opening_times = _normalize_opening_times(site.get("opening_times"))
    normalized_site = schema.NormalizedLocation(
        name=name,
        id=_make_site_id(source),
        source=source,
        address=address,
        active=True,  # this source updates weekly
        opening_dates=opening_times[0] if opening_times else None,
        opening_hours=opening_times[1] if opening_times else None,
        notes=[county] if county else None,
    )
    return normalized_site


def main():
    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])

    json_filepaths = input_dir.glob("*.ndjson")

    for in_filepath in json_filepaths:
        filename = in_filepath.name.split(".", maxsplit=1)[0]
        out_filepath = output_dir / f"{filename}.normalized.ndjson"
        logger.info(
            "Normalizing: %s => %s",
            in_filepath,
            out_filepath,
        )

        with in_filepath.open() as fin:
            with out_filepath.open("w") as fout:
                for parsed_site_json_string in fin:
                    parsed_site: dict = json.loads(parsed_site_json_string)
                    logger.debug("Parsed: %s", parsed_site)
                    normalized_site = normalize(parsed_site)
                    logger.debug("Normalized: %s", normalized_site)
                    json.dump(normalized_site.dict(exclude_unset=True), fout)
                    fout.write("\n")


if __name__ == "__main__":
    main()
