#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional, Tuple

from pydantic import ValidationError
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

ALL_DAYS = (
    schema.DayOfWeek.SUNDAY,
    schema.DayOfWeek.MONDAY,
    schema.DayOfWeek.TUESDAY,
    schema.DayOfWeek.WEDNESDAY,
    schema.DayOfWeek.THURSDAY,
    schema.DayOfWeek.FRIDAY,
    schema.DayOfWeek.SATURDAY,
)
DAY_OF_WEEK = r"(sun|mon|tues|wed|thurs|fri|sat)\.?"
AM_PM = r"(a|p)\.?m\.?"
TIME = r"\d{1,2}(:\d{1,2})?\s*" + AM_PM
TIME_RANGE = TIME + r"\s*(-|to)\s*" + TIME

# Configure logger
logger = getLogger(__file__)


def _get_access(site: dict) -> Optional[schema.Access]:
    potentials = {
        "Yes": True,
        "No": False,
    }
    # TODO: check to see if the arcgis fields really map to Access this way
    drive = potentials.get(site["attributes"]["drivethru"])
    walk = potentials.get(site["attributes"]["pedaccess"])
    if (drive, walk) != (None, None):
        return schema.Access(drive=drive, walk=walk)
    else:
        return None


# if anyone has a better way to handle these, I'm all ears.  just looked them
# up on google maps, hardcoded the parseable version.
SPECIAL_CASE_ADDRESSES = {
    # missing zipcode
    "12101 Winchester Road Sw Cumberland Allegany MD": "12101 Winchester Rd SW, Cumberland, MD 21502",
    "12154 Brittingham Lane, Princess Anne MD": "12154 Brittingham Ln, Princess Anne, MD 21853",
    "1290 East West Highway, Silver Spring, MD ": "1290 E W Hwy, Silver Spring, MD 20910",
    "1729 Dual Highway Hagerstown Washington MD": "1729 Dual Hwy, Hagerstown, MD 21740",
    "18726 N. Pointe Drive Hagerstown Washington MD": "18726 N Pointe Dr, Hagerstown, MD 21742",
    "3935 Erdman Ave #37 Baltimore MD": "3935 Erdman Ave #37, Baltimore, MD 21213",
    # missing state, and according to walgreens' site, it's the wrong zip
    "9621 Belair Rd Perry Hall 21128": "9621 Belair Rd Baltimore, MD 21236",
    # missing space between state and zip
    "1300 E North Ave Baltimore MD21213": "1300 E North Ave Baltimore MD 21213",
}
ADDRESS_RE = re.compile(
    r"^(?P<street>.*),? (?P<city>[\w \-']+),?\s+(?P<state>md|maryland),?\s+(?P<zip>\d{5}(-\d{4})?)$",
    flags=re.I,
)


def _get_address(site: dict) -> Optional[schema.Address]:
    raw = site["attributes"]["fulladdr"]
    processed = SPECIAL_CASE_ADDRESSES.get(raw, raw).strip()
    if (
        site["attributes"]["name"] == "Leidos Field at Ripken Ironbirds Stadium"
        and processed == "At this address"
    ):
        # extra paranoia for the "at this address" address
        processed = "873 Long Dr, Aberdeen, MD 21001"
    match = ADDRESS_RE.search(processed)
    if not match:
        logger.info(f"Unable to parse address string '{raw}'")
        return None
    return schema.Address(
        street1=match.group("street"),
        city=match.group("city"),
        state=schema.State.MARYLAND,
        zip=match.group("zip"),
    )


def _get_availability(site: dict) -> Optional[schema.Availability]:
    potentials = {
        "Yes": schema.Availability(drop_in=True),
        "No": schema.Availability(drop_in=False),
    }
    return potentials.get(site["attributes"]["WalkUpAvailable"])


def _get_id(site: dict) -> str:
    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    runner = "md"
    site_name = "arcgis"

    service_item = site["attributes"]["service_item_id"]
    layer = site["attributes"]["layer_id"]
    data_id = site["attributes"]["OBJECTID"]

    return f"{runner}_{site_name}:{service_item}_{layer}_{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    potentials = [
        # (attribute, Contact field)
        ("PreRegistrationURL", "website"),
        ("schedule_url", "website"),
        ("scheduling_contact_phone", "phone"),
        ("website_url", "website"),
    ]
    for attr, field in potentials:
        if site["attributes"][attr]:
            try:
                contacts.append(schema.Contact(**{field: site["attributes"][attr]}))
            except ValidationError:
                logger.debug(
                    f"Validation error adding Contact '{attr}' = '{site['attributes'][attr]}', skipping"
                )
    return contacts or None


def _strip_flavor_text(raw: str) -> str:
    parsable = [
        "Mon - Fri 7 a.m. - 11 a.m.",
        "Mon - Fri 7 a.m. - 7 p.m.",
        "Mon - Fri 7 am - 7 pm",
        "Mon - Fri 7am - 7 pm",
        "Mon - Fri 8 a.m. - 4:30 p.m.",
        "Mon - Fri 8 a.m. - 5 p.m. ",
        "Mon - Fri 8 a.m. - 5 p.m.",
        "Mon - Sat 7:30 a.m. - 5 p.m.",
        "Mon - Sun 9:00 a.m. - 5 p.m.",
        "Mon - Fri 8:30 a.m. - 6:30 p.m. Sat. 9 a.m. - 1 p.m.",
        "Mon, Tues, Thurs, Fri: 7:30am - 3pm, Wed: 3pm - 7pm",
        "Mon- Fri 7 a.m. - 11 a.m. and 2 PM-4 PM ",
        "mon- fri 7 a.m. - 11 a.m. and 2 pm-4 pm",
        "Mon- Fri 7 a.m. - 11 a.m.",
        "Sun 10am - 4pm, Mon - Fri 10am - 7pm, Sat 10am - 5pm",
        "Sun 10am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm",
        "Sun 10am - 5pm, Mon - Fri 9am - 9pm, Sat 9am - 6pm",
        "Sun 10am - 6pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm",
        "Sun 9am - 5pm, Mon - Fri 8am - 10pm, Sat 9am - 7pm",
        "Sun 9am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 7pm",
        "Wed - Sat 10am to 6pm",
        "Wed - Sun 8 a.m. to 4 p.m.",
        "wed - sun 8 a.m. - 4 p.m.",
        "wed - sun 9 a.m. - 5 p.m.",
        "Sun - Thur 9:00 a.m. - 5 p.m.",
        "tues - sat 8 a.m. - 4 p.m.",
        "tues 12:00 p.m. - 8:00 p.m., weds - sat 9:00 a.m. - 5:00 p.m.",
        "thursday-saturday,10:30 a.m. – 4:30 p.m.",
        "weds 12:00 p.m. - 8:00 p.m.; tues, thurs - sun 9:00 a.m. - 5:00 p.m.",
    ]

    parsable = [x.lower() for x in parsable]
    raw = raw.lower().replace("  ", " ")

    if re.search("covid (testing|hotline)", raw):
        # i can't parse these just reading them, never mind in code
        """
        "Mon - Fri 8 a.m. - 5 p.m. covid testing Tues. 9 a.m. - 3 p.m. covid vaccine Wed and Fri 9 a.m. - 3 p.m."
        "Mon - Fri 8 a.m. - 5 p.m. covid testing Wed 9 a.m. - 3 p.m. this week.  Tues. 9 a.m. - 3 p.m. next week. covid vaccine Mon, Wed, Thurs, Fri and Saturday this week"
        "Mon - Fri 8:30 a.m. - 4:30 p.m.covid hotline Mon - Sat 8:30 a.m. - 4:30 p.m. and Sun 8:30 a.m. - 2 p.m."
        """
        return ""
    elif re.search(r"(varies|vary)", raw):
        return ""
    elif raw.startswith("limited walk-up, no appointment slots available"):
        """
        "Limited walk-up, no appointment slots available Mon - Tues 9 a.m. - 1 p.m., Wed - Thurs 2 p.m. - 6 p.m., Fri - Sat 11 a.m. - 3 p.m."
        "Limited walk-up, no appointment slots available Weds -Sun 10 a.m. - 2 p.m."
        "Limited walk-up, no appointment slots available each day 10 a.m. - 2 p.m."
        """
        return raw.replace(
            "limited walk-up, no appointment slots available", ""
        ).strip()
    elif raw.startswith("a limited number of walk-up appointments are available"):
        """
        "A limited number of walk-up appointments are available Monday through Saturday, from 10 a.m. - 12 p.m."
        """
        return (
            raw.replace("a limited number of walk-up appointments are available", "")
            .replace(" from ", "")
            .strip()
        )
    elif raw.startswith(
        "a limited number of no-appointment vaccinations are available every day"
    ):
        return (
            raw.replace(
                "a limited number of no-appointment vaccinations are available every day",
                "",
            )
            .replace(" from ", "")
            .strip()
        )
    elif raw.startswith(
        "a limited number of no-appointment vaccinations are available"
    ):
        return (
            raw.replace("available", "")
            .replace("a limited number of no-appointment vaccinations are", "")
            .replace(" through ", "-")
            .replace(" from ", "")
            .strip()
        )

    elif raw not in parsable:
        """
        "Mon - Fri 7 a.m. - 11 a.m."
        "Mon - Fri 7 a.m. - 7 p.m."
        "Mon - Fri 7 am - 7 pm"
        "Mon - Fri 7am - 7 pm"
        "Mon - Fri 8 a.m. - 4:30 p.m."
        "Mon - Fri 8 a.m. - 5 p.m. "
        "Mon - Fri 8 a.m. - 5 p.m."
        "Mon - Fri 8:30 a.m. - 6:30 p.m. Sat. 9 a.m. - 1 p.m."
        "Mon, Tues, Thurs, Fri: 7:30am - 3pm, Wed: 3pm - 7pm"
        "Mon- Fri 7 a.m. - 11 a.m. and 2 PM-4 PM "
        "Mon- Fri 7 a.m. - 11 a.m."
        "Sun 10am - 4pm, Mon - Fri 10am - 7pm, Sat 10am - 5pm"
        "Sun 10am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm"
        "Sun 10am - 5pm, Mon - Fri 9am - 9pm, Sat 9am - 6pm"
        "Sun 10am - 6pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm"
        "Sun 9am - 5pm, Mon - Fri 8am - 10pm, Sat 9am - 7pm"
        "Sun 9am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 7pm"
        "Wed - Sat 10am to 6pm"
        """
        logger.info(f"Unable to parse hours string '{raw}'")
        return None
    else:
        return raw


def _pieces(raw: str) -> List[str]:

    """
    "Mon- Fri 7 a.m. - 11 a.m. and 2 PM-4 PM "
    """
    if not raw:
        return []

    match = re.search(
        fr"(?P<days>{DAY_OF_WEEK}\s*-\s*{DAY_OF_WEEK})\s*(?P<time1>{TIME_RANGE})\s*and\s*(?P<time2>{TIME_RANGE})",
        raw,
    )
    if match:
        return [
            f"{match.group('days')} {match.group('time1')}",
            f"{match.group('days')} {match.group('time2')}",
        ]

    """
    "Mon, Tues, Thurs, Fri: 7:30am - 3pm, Wed: 3pm - 7pm"
    """
    comma_sep_days_with_time = f"({DAY_OF_WEEK}((,|and) {DAY_OF_WEEK})*: {TIME_RANGE})"

    """
    "Sun 10am - 4pm, Mon - Fri 10am - 7pm, Sat 10am - 5pm"
    "Sun 10am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm"
    "Sun 10am - 5pm, Mon - Fri 9am - 9pm, Sat 9am - 6pm"
    "Sun 10am - 6pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm"
    "Sun 9am - 5pm, Mon - Fri 8am - 10pm, Sat 9am - 7pm"
    "Sun 9am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 7pm"
    "Mon - Tues 9 a.m. - 1 p.m., Wed - Thurs 2 p.m. - 6 p.m., Fri - Sat 11 a.m. - 3 p.m."
    "Mon - Fri 8:30 a.m. - 6:30 p.m. Sat. 9 a.m. - 1 p.m."
    """
    day_maybe_range_with_time = f"({DAY_OF_WEEK}( - {DAY_OF_WEEK})? {TIME_RANGE})"

    for pattern in (comma_sep_days_with_time, day_maybe_range_with_time):
        if re.search(pattern, raw):
            return [m[0] for m in re.findall(pattern, raw)]

    """
    "24 Hours"
    "Mon - Fri 7 a.m. - 11 a.m."
    "Mon - Fri 7 a.m. - 7 p.m."
    "Mon - Fri 7 am - 7 pm"
    "Mon - Fri 7am - 7 pm"
    "Mon - Fri 8 a.m. - 4:30 p.m."
    "Mon - Fri 8 a.m. - 5 p.m. "
    "Mon - Fri 8 a.m. - 5 p.m."
    "Mon- Fri 7 a.m. - 11 a.m."
    "Wed - Sat 10am to 6pm"
    "Monday through Saturday, from 10 a.m. - 12 p.m."
    "Weds -Sun 10 a.m. - 2 p.m."
    "each day 10 a.m. - 2 p.m."
    """
    return [raw]


def _normalize_hours(raw: str) -> Tuple[str, str]:
    match = re.search(
        r"(?P<hour>\d\d?)(:(?P<minute>\d\d))?\s*(?P<am_pm>(a|p)\.?m\.?)", raw
    )
    hour = int(match.group("hour"))
    if (1 <= hour <= 11) and (match.group("am_pm").startswith("p")):
        hour += 12
    minute = int(match.group("minute") or "0")
    return datetime.time(hour, minute).isoformat("minutes")


def _normalize_days(raw: str) -> List[str]:
    potentials = {
        "sun": schema.DayOfWeek.SUNDAY,
        "mon": schema.DayOfWeek.MONDAY,
        "tues": schema.DayOfWeek.TUESDAY,
        "wed": schema.DayOfWeek.WEDNESDAY,
        "thur": schema.DayOfWeek.THURSDAY,
        "thurs": schema.DayOfWeek.THURSDAY,
        "fri": schema.DayOfWeek.FRIDAY,
        "sat": schema.DayOfWeek.SATURDAY,
    }
    processed = raw.strip().strip(".:,")
    processed = processed.replace(" through ", " - ")

    # is it a special case?
    if processed in ("each day", "every day"):
        return list(ALL_DAYS)

    # is it a single day?
    if processed in ALL_DAYS:
        return [processed]
    elif processed in potentials:
        return [potentials[processed]]

    # is it comma-separated?
    if "," in processed:
        return sum([_normalize_days(x) for x in re.split(",|and", processed)], [])

    # is it a range?
    if "-" in processed:
        (start,), (finish,) = [_normalize_days(x) for x in processed.split("-")]
        # easier to double the ALL_DAYS list than to simulate circular array
        # for when we need to worry about stuff like "fri - mon"
        double_all_days = ALL_DAYS * 2
        start_idx = double_all_days.index(start)
        end_idx = double_all_days.index(finish, start_idx)
        return list(double_all_days[start_idx : end_idx + 1])

    logger.info(f"Unable to normalize days string '{raw}'")
    return []


def _parse_days_and_hours(raw: str) -> Optional[Tuple[List[str], Tuple[str, str]]]:
    time_match = re.search(f"(?P<time>{TIME_RANGE})", raw)
    if not (time_match):
        if "noon" in raw:
            return _parse_days_and_hours(raw.replace("noon", "12pm"))
        logger.info(f"Unable to parse time string '{raw}'")
        return None
    hours = tuple(
        _normalize_hours(h) for h in re.split("-|to", time_match.group("time"))
    )
    days = _normalize_days(raw.replace(time_match.group("time"), "").strip())
    return days, hours


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    """
    "24 Hours"
    "Mon - Fri 7 a.m. - 11 a.m."
    "Mon - Fri 7 a.m. - 7 p.m."
    "Mon - Fri 7 am - 7 pm"
    "Mon - Fri 7am - 7 pm"
    "Mon - Fri 8 a.m. - 4:30 p.m."
    "Mon - Fri 8 a.m. - 5 p.m. "
    "Mon - Fri 8 a.m. - 5 p.m. covid testing Tues. 9 a.m. - 3 p.m. covid vaccine Wed and Fri 9 a.m. - 3 p.m."
    "Mon - Fri 8 a.m. - 5 p.m. covid testing Wed 9 a.m. - 3 p.m. this week.  Tues. 9 a.m. - 3 p.m. next week. covid vaccine Mon, Wed, Thurs, Fri and Saturday this week"
    "Mon - Fri 8 a.m. - 5 p.m."
    "Mon - Fri 8:30 a.m. - 4:30 p.m.covid hotline Mon - Sat 8:30 a.m. - 4:30 p.m. and Sun 8:30 a.m. - 2 p.m."
    "Mon - Fri 8:30 a.m. - 6:30 p.m. Sat. 9 a.m. - 1 p.m."
    "Mon, Tues, Thurs, Fri: 7:30am - 3pm, Wed: 3pm - 7pm"
    "Mon- Fri 7 a.m. - 11 a.m. and 2 PM-4 PM "
    "Mon- Fri 7 a.m. - 11 a.m."
    "Sun 10am - 4pm, Mon - Fri 10am - 7pm, Sat 10am - 5pm"
    "Sun 10am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm"
    "Sun 10am - 5pm, Mon - Fri 9am - 9pm, Sat 9am - 6pm"
    "Sun 10am - 6pm, Mon - Fri 8am - 9pm, Sat 9am - 6pm"
    "Sun 9am - 5pm, Mon - Fri 8am - 10pm, Sat 9am - 7pm"
    "Sun 9am - 5pm, Mon - Fri 8am - 9pm, Sat 9am - 7pm"
    "Varies"
    "Wed - Sat 10am to 6pm"
    "A limited number of walk-up appointments are available Monday through Saturday, from 10 a.m. - 12 p.m."
    "Limited walk-up, no appointment slots available Mon - Tues 9 a.m. - 1 p.m., Wed - Thurs 2 p.m. - 6 p.m., Fri - Sat 11 a.m. - 3 p.m."
    "Limited walk-up, no appointment slots available Weds -Sun 10 a.m. - 2 p.m."
    "Limited walk-up, no appointment slots available each day 10 a.m. - 2 p.m."
    null
    """
    all_hours = []

    # TODO: try to merge operationalhours and WalkUpHours ranges that overlap?
    for attr in ("operationalhours", "WalkUpHours"):
        raw = site["attributes"][attr]
        if not raw:
            continue
        processed = raw.lower().strip()

        if processed == "24 hours":
            all_hours.extend(
                [
                    schema.OpenHour(day=d, opens="00:00", closes="23:59")
                    for d in ALL_DAYS
                ]
            )
            continue

        for piece in _pieces(_strip_flavor_text(processed)):
            result = _parse_days_and_hours(piece)
            if result:
                days, hours = result
                all_hours.extend(
                    [
                        schema.OpenHour(day=d, opens=hours[0], closes=hours[1])
                        for d in days
                    ]
                )

    return all_hours or None


def _get_notes(site: dict) -> Optional[List[str]]:
    notes = []

    for attr in ("cost_notes", "other_notes"):
        if site["attributes"][attr]:
            notes.append(site["attributes"][attr])

    # some locations mention limited number of walk-ups, which we can't capture
    # in the schema, so we bring it through as a note.
    if re.search(
        "limited (number of )?walk-up", site["attributes"]["WalkUpHours"] or "", re.I
    ):
        notes.append(site["attributes"]["WalkUpHours"])

    # some locations have hours that can be read multiple ways in english, so
    # we're not parsing them.  bring them along as a note.
    if re.search(
        "covid (testing|hotline)", site["attributes"]["operationalhours"] or "", re.I
    ):
        notes.append(site["attributes"]["operationalhours"])

    return notes or None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    id_ = _get_id(site)

    return schema.NormalizedLocation(
        id=id_,
        name=site["attributes"]["name"],
        address=_get_address(site),
        location=schema.LatLng(
            latitude=site["geometry"]["y"], longitude=site["geometry"]["x"]
        ),
        contact=_get_contacts(site),
        opening_hours=_get_opening_hours(site),
        availability=_get_availability(site),
        access=_get_access(site),
        notes=_get_notes(site),
        source=schema.Source(
            data=site,
            fetched_at=timestamp,
            fetched_from_uri=f"https://adhsgis.maps.arcgis.com/apps/opsdashboard/index.html#/{site['attributes']['service_item_id']}",  # noqa: E501
            id=id_.split(":")[-1],
            published_at=_get_published_at(site),
            source="md_arcgis",
        ),
    )


def _get_published_at(site: dict) -> str:
    return datetime.datetime.fromtimestamp(
        site["attributes"]["last_edited_date"] / 1000
    ).isoformat()


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.ndjson")

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

for in_filepath in json_filepaths:
    filename, _ = os.path.splitext(in_filepath.name)
    out_filepath = output_dir / f"{filename}.normalized.ndjson"

    logger.info(
        "normalizing %s => %s",
        in_filepath,
        out_filepath,
    )

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for site_json in fin:
                parsed_site = json.loads(site_json)

                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
