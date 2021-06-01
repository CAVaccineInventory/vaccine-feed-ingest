#!/usr/bin/env python

import datetime
import os
import pathlib
import re
import sys
from typing import Any, List, Optional

import orjson
import us
from vaccine_feed_ingest_schema import location as schema
from vaccine_feed_ingest_schema.common import BaseModel

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import normalize_phone, normalize_zip

# Performance optimization: skip validation in our pydantic models.
#
# This is safe to do because the ingestion framework will do the same
# validation after running us.
OPTIMIZED = True


def _create_instance(cls, *args, **kwds):
    if OPTIMIZED:
        return cls.construct(*args, **kwds)
    else:
        return cls(*args, **kwds)


BaseModel.create = classmethod(_create_instance)


logger = getLogger(__file__)


def _get_id(site: dict) -> str:
    data_id = site["id"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "carbon_health"
    runner = "us"

    return f"{runner}_{site_name}:{data_id}"


def _get_filter(site: dict, key: str, default: Any) -> Any:
    try:
        return site["services"][0]["filters"][key]
    except (IndexError, KeyError):
        return default


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    location_url = _get_filter(site, "locationUrl", "")

    contacts = []

    if site["phoneNumber"]:
        for phone in normalize_phone(site["phoneNumber"]):
            contacts.append(phone)

    def cleanup_url(url):
        if not url or not url.strip():
            return None
        if url in ["No website", "Website link", "Booking Registration link"]:
            return None
        if "@" in url:
            # Some of these are email addresses.
            # Skipping those for now.
            return None

        url = re.sub(r"^(http|https):/(\w+)", r"\1://\2", url)
        url = re.sub(r"^www.https://", "https://", url)
        url = re.sub(r"^https//", "https://", url)
        url = re.sub(r"^https:(\w+)", r"https://\1", url)
        url = re.sub(
            r"^https://wynne apothecary.com", "https://wynneapothecary.com", url
        )

        # workaround until samuelcolvin/pydantic#2778 is merged
        url = url.rstrip("#")

        if not url.startswith("http"):
            url = "http://" + url

        return url

    if location_url == "healthyguilford.com, conehealth.com/vaccine":
        contacts.append(schema.Contact.create(website="http://healthyguilford.com"))
        contacts.append(schema.Contact.create(website="https://conehealth.com/vaccine"))
    # A few sites have "locationUrl" set to something like this:
    # `https://myvaccine.fl.gov/ or 866-201-6313`
    elif match := re.match(
        r"^(https://\S+) or (\d\d\d-\d\d\d-\d\d\d\d)$", location_url
    ):
        contacts.append(schema.Contact.create(phone=match[2]))
        contacts.append(schema.Contact.create(website=match[1]))
    elif url := cleanup_url(location_url):
        contacts.append(schema.Contact.create(website=url))

    if registration_link := cleanup_url(_get_filter(site, "registrationLink", "")):
        contacts.append(schema.Contact.create(website=registration_link))

    if contacts:
        return contacts

    return None


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    open_hours = []

    def _parse_time(t: str) -> Optional[datetime.time]:
        # example: "8:30AM"
        if match := re.match(r"(\d\d?):(\d\d)(AM|PM|am|pm)", t):
            hh = int(match[1])
            if hh == 12:
                hh = 0
            if 0 <= hh <= 11 and 0 <= int(match[2]) <= 59:
                return datetime.time(
                    hh + (12 if match[3] in ["pm", "PM"] else 0),
                    int(match[2]),
                )
        return None

    days_by_index = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]

    for hours in _get_filter(site, "hours", []):
        opens = _parse_time(hours["from"])
        closes = _parse_time("11:59PM" if hours["to"] == "12:00AM" else hours["to"])
        if opens and closes:
            if opens < closes:
                open_hours.append(
                    schema.OpenHour.create(
                        day=days_by_index[hours["day"] - 1],
                        opens=opens,
                        closes=closes,
                    )
                )
            else:
                logger.info(
                    "ignoring incorrect open hours: opens=%s closes=%s", opens, closes
                )

    if open_hours:
        return open_hours

    return None


def _get_availability(site: dict) -> Optional[schema.Availability]:
    if _get_filter(site, "acceptsWalkIns", "") == "1":
        return schema.Availability.create(drop_in=True)


def _get_state(site: dict) -> Optional[str]:
    if state := us.states.lookup(site["state"]):
        return state.abbr
    return None


def _get_zip(site: dict) -> Optional[str]:
    code = site["zip"]
    if code and code.isdecimal() and len(code) < 5:
        code = code.zfill(5)
    if re.match(r"[0-9]{5}-", code):
        code = code.rstrip("-")
    code = normalize_zip(code)
    if code and schema.ZIPCODE_RE.match(code):
        return code
    if site["zip"]:
        logger.info("ignoring invalid zip code '%s'", site["zip"])
    return None


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    vaccine_names = [
        ("Pfizer", "pfizer_biontech"),
        ("Moderna", "moderna"),
        ("Janssen", "johnson_johnson_janssen"),
    ]

    vaccines = []
    for their_name, our_name in vaccine_names:
        if _get_filter(site, f"offers{their_name}", "") == "1":
            vaccines.append(schema.Vaccine.create(vaccine=our_name))

    if vaccines:
        return vaccines

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    if notes := _get_filter(site, "providerNotes", ""):
        return [notes]
    return None


def _get_normalized_location(
    site: dict, timestamp: str, filename: str
) -> schema.NormalizedLocation:
    return schema.NormalizedLocation.create(
        id=_get_id(site),
        name=site["name"],
        address=schema.Address.create(
            street1=site["firstLine"],
            street2=None,
            city=site["city"],
            state=_get_state(site),
            zip=_get_zip(site),
        ),
        location=schema.LatLng.create(
            latitude=site["x"],
            longitude=site["y"],
        ),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=_get_opening_hours(site),
        availability=_get_availability(site),
        inventory=_get_inventory(site),
        access=None,
        parent_organization=None,
        links=None,
        notes=_get_notes(site),
        active=None,
        source=schema.Source.create(
            source="us_carbon_health",
            id=site["id"],
            fetched_from_uri=f"https://carbonhealth.com/static/data/rev/{filename}",
            fetched_at=timestamp,
            published_at=None,
            data=site,
        ),
    )


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

    with in_filepath.open("rb") as fin:
        with out_filepath.open("wb") as fout:
            # Optimization: faster to batch all of the sites into a single
            # write than to do ~46k separate writes.
            # Optimization: using orjson rather than json.
            fout.write(
                b"\n".join(
                    orjson.dumps(
                        _get_normalized_location(
                            orjson.loads(site_json),
                            parsed_at_timestamp,
                            filename,
                        ).dict()
                    )
                    for site_json in fin
                )
            )
            fout.write(b"\n")
