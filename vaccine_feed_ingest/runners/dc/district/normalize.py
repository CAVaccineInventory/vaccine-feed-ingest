#!/usr/bin/env python

import datetime
import json
import pathlib
import re
import sys
from hashlib import md5
from typing import List, Optional

import dateparser
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

MONTHS = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "novermber",
    "december",
]
MONTHS_PATTERN = f"{'|'.join(MONTHS)}"
DATE_RANGE_PATTERN = r"\d{1,2}\s*-\s*\d{1,2}"  # eg "6-12"
DATE_SINGLE_OR_RANGE_PATTERN = fr"{DATE_RANGE_PATTERN}|\d{{1,2}}"  # eg "6", "6-12"
MONTH_DATE_TO_MONTH_DATE_PATTERN = fr"(?P<start_date>(?:{MONTHS_PATTERN})\s+\d{{1,2}})\s*-\s*(?P<end_date>(?:{MONTHS_PATTERN})\s*\d{{1,2}})"  # eg "May 3 - May 5"
MONTH_DATE_TO_DATE_PATTERN = fr"(?P<month>({MONTHS_PATTERN}))\s+(?P<start_date>\d{{1,2}})\s*-\s*(?P<end_date>\d{{1,2}})"  # eg "May 3 - 5"
SCHEMA_DAYS = [
    x.value for x in schema.DayOfWeek if x != schema.DayOfWeek.PUBLIC_HOLIDAYS
]
DOUBLE_DAYS = (
    SCHEMA_DAYS + SCHEMA_DAYS
)  # for day ranges that wrap over a weekend, eg. fri-mon
DAYS_OF_WEEK = SCHEMA_DAYS + [
    "mon",
    "tue",
    "tues",
    "wed",
    "thu",
    "thur",
    "fri",
    "sat",
    "sun",
]
DAYS_OF_WEEK_PATTERN = f"{'|'.join(DAYS_OF_WEEK)}"
HOURS_PATTERN = r"\d{1,2}(?::\d{1,2})?"  # eg "10", "1:23"
AM_PM_PATTERN = r"[ap]\.?m\.?"  # eg "am", "a.m."
HOURS_RE = re.compile(
    fr"(?P<hours>\d{{1,2}})(?::(?P<minutes>\d{{1,2}}))?\s*(?P<am_pm>{AM_PM_PATTERN})",
    re.I,
)  # eg "1pm", "2:34 am"
HOURS_RANGE_RE = re.compile(
    fr"(?P<range>\d{{1,2}}(?::\d{{1,2}})?\s*(?:{AM_PM_PATTERN})\s*-\s*\d{{1,2}}(?::\d{{1,2}})?\s*(?:{AM_PM_PATTERN}))"
)  # eg "9am - 5pm"
VACCINES_PATTERN = "pfizer|moderna|j&j"


def _get_address(site: dict) -> schema.Address:
    return schema.Address(
        street1=site["Address"],
        city="Washington",
        state=schema.State.DISTRICT_OF_COLUMBIA,
    )


def _get_opening_dates(site: dict) -> Optional[List[schema.OpenDate]]:
    """
    "May 3 - May 6 and May 10 - May 13 \n9am-1pm \nMay 24 - May 27 \n2pm - 7pm"
    "May 3-May 6 and May 10-May13 \n9am-1pm \nMay 24-May 27 \n2pm-7pm"
    "May 5-May 8: 9am-1pm \nMay 12-15, 19-22, 26-29: 2pm-7pm"
    "May 3: 9am-1pm \nMay 6-8, 10, 13-15, 17, 20-22, 24: 2pm-7pm \nMay 27-29: 9am-1pm"
    "May 5, 6, 12, 14, 19, 20 \n9am-2pm"
    """
    opening_dates = []
    days_hours = site["Normal Days / Hours"].lower().replace("and", ", ")

    # short-circuit if it's an entry with days, not dates
    if not re.search(MONTHS_PATTERN, days_hours):
        return None

    # split on the "9am-1pm"s (leading ":" is optional, giving us just the
    # date entries
    entries = re.split(
        fr"(?::\s*)?(?:{HOURS_PATTERN})\s*(?:{AM_PM_PATTERN})\s*-\s*(?:{HOURS_PATTERN})\s*(?:{AM_PM_PATTERN})",
        days_hours,
    )
    for entry in entries:
        entry_match = re.search(MONTH_DATE_TO_MONTH_DATE_PATTERN, entry)
        if entry_match:
            for start_date, end_date in re.findall(
                MONTH_DATE_TO_MONTH_DATE_PATTERN, entry
            ):
                # "May 5-May 8"
                start_date = dateparser.parse(start_date).date().isoformat()
                end_date = dateparser.parse(end_date).date().isoformat()
                opening_dates.append(schema.OpenDate(opens=start_date, closes=end_date))
            continue

        entry_match = re.search(
            fr"(?P<month>({MONTHS_PATTERN}))\s+(?P<dates>({DATE_SINGLE_OR_RANGE_PATTERN})(\s*,\s*({DATE_SINGLE_OR_RANGE_PATTERN}))*)",
            entry,
        )
        if entry_match:
            # "May 6-8, 10, 13-15, 17, 20-22, 24"
            # "May 11"
            month = entry_match.group("month")
            pieces = entry_match.group("dates")
            for piece in re.split(r"\s*,\s*", pieces):
                if "-" in piece:
                    start, end = re.split(r"\s*-\s*", piece)
                    start_date = dateparser.parse(f"{month} {start}").date().isoformat()
                    end_date = dateparser.parse(f"{month} {end}").date().isoformat()
                    opening_dates.append(
                        schema.OpenDate(opens=start_date, closes=end_date)
                    )
                else:
                    date = dateparser.parse(f"{month} {piece}").date().isoformat()
                    opening_dates.append(schema.OpenDate(opens=date, closes=date))
            continue

        if entry:
            logger.info(f"Unparseable opening_dates: {entry}")

    return opening_dates or None


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    opening_hours = []
    days_hours = site["Normal Days / Hours"].lower()

    # short-circuit if it's an entry with dates, not days
    if not re.search(DAYS_OF_WEEK_PATTERN, days_hours):
        return None

    # there's "."s after abbreviated days of the week in the one with
    # different vaccines on different days.  just nuke 'em.
    if re.search(VACCINES_PATTERN, days_hours):
        days_hours = days_hours.replace(".", "").replace(" & ", "/")

    # split on the parenthesized vaccine if it's there, otherwise the whole
    # of `days_hours` is a single `piece`
    for piece in re.split(fr"\s*\((?:{VACCINES_PATTERN})\)\s*", days_hours):
        if not piece:
            continue

        if re.search(
            f"({DAYS_OF_WEEK_PATTERN})(/({DAYS_OF_WEEK_PATTERN}))+",
            piece,
        ):
            """
            "Tues/Wed/Fri  \n10am-4pm"
            "Tues/Wed/Fri 10am - 4pm"
            """
            days_match = re.search(
                f"(?P<days>(?:{DAYS_OF_WEEK_PATTERN})(?:/(?:{DAYS_OF_WEEK_PATTERN}))+)",
                piece,
            )
            days = _normalize_days(re.split(r"\s*/\s*", days_match.group("days")))
            for hours_range in HOURS_RANGE_RE.findall(piece):
                opens, closes = [
                    _normalize_time(*m) for m in HOURS_RE.findall(hours_range)
                ]
                opening_hours.extend(
                    [
                        schema.OpenHour(
                            day=d, opens=opens.isoformat(), closes=closes.isoformat()
                        )
                        for d in days
                    ]
                )
        elif re.search(
            rf"({DAYS_OF_WEEK_PATTERN})\s*-\s*({DAYS_OF_WEEK_PATTERN})", piece
        ):
            """
            "Thursday-Sunday \n8am-12pm, 1pm-5pm"
            "Monday-Thursday \n9am-3pm"
            "Monday-Saturday \n9am-5pm"
            "Tuesday - Sunday\n10am - 2pm"
            """
            days_match = re.search(
                rf"(?P<days>({DAYS_OF_WEEK_PATTERN})\s*-\s*({DAYS_OF_WEEK_PATTERN}))",
                piece,
            )
            start_day, end_day = _normalize_days(
                re.split(r"\s*-\s*", days_match.group("days"))
            )
            start_idx = DOUBLE_DAYS.index(start_day)
            end_idx = DOUBLE_DAYS.index(end_day, start_idx)
            days = DOUBLE_DAYS[start_idx : end_idx + 1]
            for hours_range in HOURS_RANGE_RE.findall(piece):
                opens, closes = [
                    _normalize_time(*m) for m in HOURS_RE.findall(hours_range)
                ]
                opening_hours.extend(
                    [
                        schema.OpenHour(
                            day=d, opens=opens.isoformat(), closes=closes.isoformat()
                        )
                        for d in days
                    ]
                )
        else:
            logger.info(f'Unable to parse opening hours from "{piece}"')

    return opening_hours or None


def _get_id(site: dict) -> str:
    # it's just an html table, best we can do is hash the name
    data_id = md5(site["Walk-Up Site"].encode("utf-8")).hexdigest()
    site_name = "district"
    runner = "dc"
    return f"{runner}_{site_name}:{data_id}"


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    inventory = []
    # one site has different vaccines on different days, so we need to check
    # the hours field as well as the vaccine field
    for field in ("Vaccine", "Normal Days / Hours"):
        if re.search("pfizer", field, re.I):
            inventory.append(schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH))
        elif re.search("moderna", field, re.I):
            inventory.append(schema.Vaccine(vaccine=schema.VaccineType.MODERNA))
        elif re.search("johnson & johnson", field, re.I):
            inventory.append(
                schema.Vaccine(vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN)
            )
    return inventory or None


def _get_notes(site: dict) -> Optional[List[str]]:
    notes = []

    # our data model doesn't handle "days of month with times", so we record
    # the days of the month as `opening_dates`, and stick the original in as
    # a note.
    days_hours = site["Normal Days / Hours"]
    if re.search(MONTHS_PATTERN, days_hours.lower()):
        notes.append(days_hours)

    return notes or None


def _normalize_days(raw_days: str) -> List[str]:
    potentials = {
        "mon": schema.DayOfWeek.MONDAY,
        "tue": schema.DayOfWeek.TUESDAY,
        "tues": schema.DayOfWeek.TUESDAY,
        "wed": schema.DayOfWeek.WEDNESDAY,
        "thu": schema.DayOfWeek.THURSDAY,
        "thur": schema.DayOfWeek.THURSDAY,
        "fri": schema.DayOfWeek.FRIDAY,
        "sat": schema.DayOfWeek.SATURDAY,
        "sun": schema.DayOfWeek.SUNDAY,
    }
    processed = []
    for raw in raw_days:
        if raw in SCHEMA_DAYS:
            processed.append(raw)
        elif raw in potentials:
            processed.append(potentials[raw])
    return processed


def _normalize_time(hour: str, minute: str, am_pm: str) -> datetime.time:
    hour = int(hour)
    minute = int(minute or "0")
    if (1 <= hour <= 11) and am_pm.startswith("p"):
        hour += 12
    return datetime.time(hour, minute)


def normalize(site: dict, timestamp: str) -> schema.NormalizedLocation:
    id_ = _get_id(site)

    return schema.NormalizedLocation(
        id=id_,
        name=site["Walk-Up Site"],
        address=_get_address(site),
        opening_dates=_get_opening_dates(site),
        opening_hours=_get_opening_hours(site),
        availability=schema.Availability(appointments=False, drop_in=True),
        inventory=_get_inventory(site),
        notes=_get_notes(site),
        source=schema.Source(
            source="dc_district",
            id=id_.split(":")[-1],
            fetched_from_uri="https://coronavirus.dc.gov/vaccinatedc",
            fetched_at=timestamp,
            data=site,
        ),
    )


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"

with input_file.open() as fin:
    with output_file.open("w") as fout:
        for line in fin:
            site = json.loads(line)
            normalized_site = normalize(site, parsed_at_timestamp)
            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
