#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional, Tuple

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.validation import BOUNDING_BOX

logger = getLogger(__file__)

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.ndjson")

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["globalid"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis"
    runner = "az"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "128ead309d754558ad81bccd99188dc9"
    layer = 0

    return f"{runner}_{site_name}:{arcgis}_{layer}_{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["prereg_phone"]:
        matches = list(
            re.finditer(
                r"(?P<area_code>\d\d\d)\)?-? ?(?P<rest_of_number>\d\d\d-\d\d\d\d)",
                site["attributes"]["prereg_phone"],
            )
        )

        if not matches:
            logger.warning(
                "unparseable phone number: '%s'", site["attributes"]["prereg_phone"]
            )
            return None

        for match in matches:
            phone = f"({match.group('area_code')}) {match.group('rest_of_number')}"
            contacts.append(schema.Contact(contact_type="general", phone=phone))

    website = site["attributes"]["prereg_website"]
    if website:
        # this edge case...
        website = website.replace("htttp", "http")
        if "http" not in website:
            website = "https://" + website
        website = website.replace(" ", "")
        contacts.append(schema.Contact(contact_type="general", website=website))

    if len(contacts) > 0:
        return contacts

    return None


def _get_languages(site: dict) -> Optional[List[str]]:
    return {None: None, "Yes": ["en", "es"], "No": ["en"]}[
        site["attributes"]["spanish_staff_y_n"]
    ]


def _get_opening_dates(site: dict) -> Optional[List[schema.OpenDate]]:
    opens = None
    closes = None
    if site["attributes"]["begindate"] is not None:
        opens = (
            datetime.datetime.fromtimestamp(site["attributes"]["begindate"] // 1000)
            .date()
            .isoformat()
        )

    if site["attributes"]["enddate"] is not None:
        closes = (
            datetime.datetime.fromtimestamp(site["attributes"]["enddate"] // 1000)
            .date()
            .isoformat()
        )

    if opens is None and closes is None:
        return None

    return [
        schema.OpenDate(
            opens=opens,
            closes=closes,
        )
    ]


TIME_RANGE_RE = re.compile(
    r"(?P<hour>\d{1,2})(:(?P<minute>\d{1,2}))?\s*(?P<am_pm>[AP]\.?M\.?)?"
)


def _parse_time(human_readable_time: str) -> Tuple[int, int]:
    match = TIME_RANGE_RE.search(human_readable_time)
    if match:
        hour = int(match.group("hour"))
        minute = int(match.group("minute") or "0")
        if (1 <= hour <= 11) and ((match.group("am_pm") or "").startswith("P")):
            hour += 12
        return hour, minute
    raise ValueError(human_readable_time)


def _normalize_time(human_readable_time: str) -> str:
    hour, minute = _parse_time(human_readable_time)
    return datetime.time(hour % 24, minute).isoformat("minutes")


def _normalize_hours(
    human_readable_hours: Optional[str], day: str
) -> List[schema.OpenHour]:
    processed_hours = human_readable_hours
    if processed_hours is None:
        return []
    processed_hours = processed_hours.upper()

    if processed_hours == "8-4":
        return [schema.OpenHour(day=day, opens="08:00", closes="16:00")]
    elif processed_hours == "8:00AM7:00PM":
        return [schema.OpenHour(day=day, opens="08:00", closes="19:00")]
    elif processed_hours.startswith("10:00PM") and processed_hours.count("PM") == 2:
        # "sat_hrs" for multiple Safeway locations are PM to PM, and
        # "sun_hrs" is the same hours, but AM to PM.  just fix the typo.
        processed_hours = processed_hours.replace("PM", "AM", 1)

    processed_hours = re.sub("^BY APPOINTMENT", "", processed_hours).strip()

    if " AND " in processed_hours:
        ranges = processed_hours.split(" AND ")
        return sum((_normalize_hours(hours_range, day) for hours_range in ranges), [])

    if ";" in processed_hours:
        ranges = processed_hours.split(";")
        return sum((_normalize_hours(hours_range, day) for hours_range in ranges), [])

    if " TO " in processed_hours:
        processed_hours = processed_hours.replace(" TO ", "-")

    if processed_hours.count("-") != 1:
        logger.warning("unparseable hours: '%s'", human_readable_hours)
        return []

    open_time, close_time = [x.strip() for x in re.split(r"\s*-\s*", processed_hours)]
    opens = _normalize_time(open_time)
    closes = _normalize_time(close_time)

    # handle the "9-5" case, where the PM for 5 is implied
    if (opens > closes) and not re.search(r"P\.?M\.?$", close_time):
        original_closes = datetime.time.fromisoformat(closes)
        closes = original_closes.replace(hour=original_closes.hour + 12).isoformat()

    try:
        return [
            schema.OpenHour(
                day=day,
                opens=opens,
                closes=closes,
            )
        ]
    except ValueError:
        logger.exception("unparseable hours: '%s'", human_readable_hours)
        return []


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    hours = []

    if site["attributes"]["mon_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["mon_hrs"], "monday")

    if site["attributes"]["tues_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["tues_hrs"], "tuesday")

    if site["attributes"]["wed_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["wed_hrs"], "wednesday")

    if site["attributes"]["thurs_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["thur_hrs"], "thursday")

    if site["attributes"]["fri_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["fri_hrs"], "friday")

    if site["attributes"]["sat_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["sat_hrs"], "saturday")

    if site["attributes"]["sun_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["sun_hrs"], "sunday")

    return hours if hours else None


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    inventory_str = site["attributes"]["vaccine_manufacturer"]
    inventory = (
        inventory_str.split(";") if ";" in inventory_str else inventory_str.split(",")
    )

    return [
        {
            "Pfizer_BioNTech": schema.Vaccine(vaccine="pfizer_biontech"),
            "Pfizer-BioNTech": schema.Vaccine(vaccine="pfizer_biontech"),
            "Pfizer": schema.Vaccine(vaccine="pfizer_biontech"),
            "Moderna": schema.Vaccine(vaccine="moderna"),
            "J_J": schema.Vaccine(vaccine="johnson_johnson_janssen"),
        }[vaccine.lstrip("\u200b").strip()]
        for vaccine in inventory
    ]


def _get_lat_lng(site: dict) -> Optional[schema.LatLng]:
    lat_lng = schema.LatLng(
        latitude=site["geometry"]["y"], longitude=site["geometry"]["x"]
    )

    # Some locations in the AZ data set have lat/lng near the south pole. Drop
    # those values.
    if not BOUNDING_BOX.latitude.contains(
        lat_lng.latitude
    ) or not BOUNDING_BOX.longitude.contains(lat_lng.longitude):
        return None

    return lat_lng


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["loc_name"],
        address=schema.Address(
            street1=site["attributes"]["addr1"],
            street2=site["attributes"]["addr2"],
            city=site["attributes"]["city"],
            state="AZ",
            zip=site["attributes"]["zip"],
        ),
        location=_get_lat_lng(site),
        contact=_get_contacts(site),
        languages=_get_languages(site),
        opening_dates=_get_opening_dates(site),
        opening_hours=_get_opening_hours(site),
        availability=None,
        inventory=_get_inventory(site),
        access=None,
        parent_organization=None,
        links=None,
        notes=[site["attributes"]["prereg_comments"]]
        if site["attributes"]["prereg_comments"]
        else None,
        active=None,
        source=schema.Source(
            source="az_arcgis",
            id=site["attributes"]["globalid"],
            fetched_from_uri="https://adhsgis.maps.arcgis.com/apps/opsdashboard/index.html#/5d636af4d5134a819833b1a3b906e1b6",  # noqa: E501
            fetched_at=timestamp,
            data=site,
        ),
    )


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

                if parsed_site["attributes"]["addr1"] is None:
                    continue
                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
