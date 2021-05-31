#!/usr/bin/env python

import datetime
import json
import pathlib
import sys
from typing import List, Tuple

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.parse import location_id_from_name

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _get_name(site: dict) -> str:
    return site["name"][0]


def _get_city(site: dict) -> str:
    address_string = site["address"]

    city = None
    split_address = address_string.splitlines()
    if len(split_address) == 2:
        if "Texas" in split_address[1]:
            split_string = ", Texas"
        else:
            split_string = ", TX"

        city = split_address[1].split(split_string)[0].strip()

    return city


def _get_address(site: dict) -> schema.Address:
    address_string = site["address"]

    # If the address string isn't two lines, I don't know how they would format it
    # and want to avoid adding mangled data to the database
    split_address = address_string.splitlines()
    if len(split_address) == 2:
        if "Texas" in split_address[1]:
            split_string = ", Texas"
        else:
            split_string = ", TX"

        street1 = split_address[0]
        city = split_address[1].split(split_string)[0].strip()
        state = schema.State.TEXAS
        zip = split_address[1].split(split_string)[-1].strip()

        address = schema.Address(street1=street1, city=city, state=state, zip=zip)
        return address
    else:
        return None


def _get_schema_day(day: str) -> schema.DayOfWeek:
    day_lower = day.lower()

    if "mon" in day_lower:
        return schema.DayOfWeek.MONDAY
    if "tues" in day_lower:
        return schema.DayOfWeek.TUESDAY
    if "wed" in day_lower:
        return schema.DayOfWeek.WEDNESDAY
    if "thur" in day_lower:
        return schema.DayOfWeek.THURSDAY
    if "fri" in day_lower:
        return schema.DayOfWeek.FRIDAY
    if "sat" in day_lower:
        return schema.DayOfWeek.SATURDAY
    if "sun" in day_lower:
        return schema.DayOfWeek.SUNDAY


def _clean_hour_string(hour_string: str) -> str:
    hour_string = hour_string.strip().lower()

    return hour_string


def _time_to_digits(time_str: str) -> Tuple[str, str]:
    time = time_str.replace("pm", "")
    time = time.replace("p.m.", "")
    time = time.replace("am", "")
    time = time.replace("a.m.", "")
    time = time.strip()
    hour, minute = time.split(":")

    return hour, minute


def _process_single_time(single_time: str) -> str:
    single_time = _clean_hour_string(single_time)
    if "p.m" in single_time or "pm" in single_time:
        hour, minute = _time_to_digits(single_time)
        if int(hour) != 12:
            hour = str(int(hour) + 12)
    else:
        hour, minute = _time_to_digits(single_time)
        if hour == 12:
            hour = "0"

    time_processed = "{}:{}".format(hour, minute)

    return time_processed


def _parse_time_string(time_string: str) -> Tuple[str, str]:
    start_str, end_str = time_string.split("-")

    start_processed = _process_single_time(start_str)
    end_processed = _process_single_time(end_str)

    return start_processed, end_processed


def _get_hours(site: dict) -> List[schema.OpenHour]:
    hour_list = site["hours"]
    open_hours = []
    for day_of_week in hour_list:
        days = day_of_week[0]
        times = day_of_week[1]

        days = days.split("–")
        schema_days = []
        if len(days) == 2:
            day1 = days[0].strip()
            day2 = days[-1].strip()

            day1_index = DAYS.index(day1)
            day2_index = DAYS.index(day2)

            # Could do this with modular arithmetic, but it would be less readable
            if day1_index <= day2_index:
                for i in range(day1_index, day2_index + 1):
                    schema_days.append(_get_schema_day(DAYS[i]))
            else:
                for i in range(day1_index, len(DAYS)):
                    schema_days.append(_get_schema_day(DAYS[i]))
                for i in range(0, day2_index + 1):
                    schema_days.append(_get_schema_day(DAYS[i]))

        elif len(days) == 1:
            schema_day = _get_schema_day(days[0].strip())
            schema_days.append(schema_day)

        for time in times:
            start_time, end_time = _parse_time_string(time)

            for schema_day in schema_days:
                open_hour = schema.OpenHour(
                    day=schema_day, opens=start_time, closes=end_time
                )
                open_hours.append(open_hour)

    return open_hours


def _get_vaccine(site: dict) -> List[schema.Vaccine]:
    vaccine_string = site["vaccine"].lower()

    vaccines = []

    if "moderna" in vaccine_string:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.MODERNA))
    if "pfizer" in vaccine_string:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH))
    if "johnson" in vaccine_string:
        vaccines.append(
            schema.Vaccine(vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN)
        )
    if "oxford" in vaccine_string:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.OXFORD_ASTRAZENECA))

    return vaccines


def _get_notes(site: dict) -> List[str]:
    return [site["location_details"]]


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://memorialhermann.org/services/conditions/coronavirus/vaccine-walk-in-clinics",
        id=_get_id(site),
        source="tx_memorialhermann",
    )


def _get_id(site: dict) -> str:
    name = _get_name(site)
    city = _get_city(site)

    id = location_id_from_name(f"{name}_{city}")

    return id


def normalize(site: dict, timestamp: str) -> dict:
    normalized = schema.NormalizedLocation(
        id=("tx_memorialhermann:" + _get_id(site)),
        name=_get_name(site),
        address=_get_address(site),
        opening_hours=_get_hours(site),
        inventory=_get_vaccine(site),
        notes=_get_notes(site),
        availability=schema.Availability(
            drop_in=True
        ),  # The webpage is for walk-in clinics
        source=_get_source(site, timestamp),
    ).dict()

    return normalized


def normalize_from_list(sites: list, timestamp: str) -> List[str]:
    ret = []
    for site in sites:
        ret.append(normalize(site, timestamp))
    return ret


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)["sites"]

            normalized_sites = []
            if isinstance(site_blob, list):
                normalized_sites = normalize_from_list(site_blob, parsed_at_timestamp)
            else:
                normalized_sites = normalize_from_list([site_blob], parsed_at_timestamp)

            for normalized_site in normalized_sites:
                json.dump(normalized_site, fout)
                fout.write("\n")
