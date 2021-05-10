#!/usr/bin/env python

import datetime
import json
import pathlib
import re
import sys
from typing import List, Optional

import dateparser
from bs4 import BeautifulSoup
from pydantic import ValidationError
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


def _get_access(site: dict) -> Optional[schema.Access]:
    if "wheelchair" in site["description"].lower():
        return schema.Access(wheelchair=schema.WheelchairAccessLevel.YES)
    return None


def _get_address(site: dict) -> schema.Address:
    return schema.Address(
        street1=site["address_1"],
        street2=site["address_2"],
        city=site["city"],
        state=site["state"],
        zip=site["postal_code"],
    )


def _get_availability(site: dict) -> Optional[schema.Availability]:
    drop_in, appointments = None, None

    for field in ("description", "application_process", "hours_of_operation"):
        value = site[field].lower()
        if "appointment" in value:
            appointments = True
        if re.search("walk[ |-]in", value):
            drop_in = True

    if drop_in or appointments:
        return schema.Availability(drop_in=drop_in, appointments=appointments)
    else:
        return None


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    """
    "site_phones": {
        "phone_1": "860-827-7690",
        "phone_2": "833-943-5721",
        "phone_1_name": "Voice",
        "phone_2_name": "Voice",
        "phone_1_description": "Hartford HealthCare Access Center (M-F: 8am-5pm)",
        "phone_2_description": "Hartford HealthCare Access Center (M-F: 8am-5pm)",
        "phone_toll_free_description": ""
    },
    """
    phones = [k for k in site["site_phones"] if re.match(r"phone_\d+", k)]
    for phone in phones:
        try:
            contacts.append(
                schema.Contact(
                    phone=site["site_phones"][phone],
                )
            )
        except ValidationError:
            logger.debug("Invalid phone {site['site_phones'][phone]}")

    if site.get("agency_phones", {}).get("phone_tty", None):
        contacts.append(
            schema.Contact(
                other=f"{site['agency_phones']['phone_tty']} TTY",
            )
        )

    # the site and agency website are frequently the same, dedupe 'em
    websites = set()
    for key in ("agency_website", "site_website"):
        if site[key]:
            websites.add(site[key])
    for website in websites:
        if not website.startswith("http"):
            website = f"https://{website}"
        try:
            contacts.append(
                schema.Contact(
                    website=website,
                )
            )
        except ValidationError:
            logger.debug("Invalid website {website}")

    return contacts or None


def _get_id(site: dict) -> str:
    return f"ct_state:{site['number']}"


def _get_languages(site: dict) -> Optional[List[str]]:
    potentials = {
        "spanish": "es",
        "french": "fr",
        "polish": "pl",
    }

    languages = []
    for lang in re.split(r"\s*,\s*", site.get("languages") or ""):
        if lang.lower() in potentials:
            languages.append(potentials[lang.lower()])
    return languages or None


def _get_location(site: dict) -> Optional[schema.LatLng]:
    float_pattern = r"-?\d+\.\d+"
    match = re.search(
        f"(?P<lng>{float_pattern}) (?P<lat>{float_pattern})", site["location"]
    )
    if match:
        """
        "POINT (-73.04276 41.55975)"
        """
        return schema.LatLng(
            latitude=float(match.group("lat")),
            longitude=float(match.group("lng")),
        )
    else:
        return None


def _get_normalized_location(
    site: dict, timestamp: str
) -> Optional[schema.NormalizedLocation]:
    id_ = _get_id(site)
    return schema.NormalizedLocation(
        id=id_,
        name=site["name"],
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        languages=_get_languages(site),
        opening_hours=_get_opening_hours(site),
        availability=_get_availability(site),
        access=_get_access(site),
        notes=_get_notes(site),
        source=schema.Source(
            source="ct_state",
            id=id_.split(":")[-1],
            fetched_from_uri="https://www.211ct.org/search",
            fetched_at=timestamp,
            published_at=_get_published_at(site),
            data=site,
        ),
    )


def _get_notes(site: dict) -> Optional[List[str]]:
    notes = []

    if site["description"]:
        notes.append(BeautifulSoup(site["description"], "html.parser").text)

    for field in ("eligibility", "application_process", "documents_required"):
        if site.get(field):
            notes.append(f"{field}: {site[field]}")

    return notes or None


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    return None


def _get_published_at(site: dict) -> str:
    return (
        dateparser.parse(site["updated_at"])
        .astimezone(datetime.timezone.utc)
        .isoformat()
    )


output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"
input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

with input_file.open() as fin:
    with output_file.open("w") as fout:
        for site_json in fin:
            parsed_site = json.loads(site_json)

            normalized_site = _get_normalized_location(parsed_site, parsed_at_timestamp)
            if not normalized_site:
                continue

            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
