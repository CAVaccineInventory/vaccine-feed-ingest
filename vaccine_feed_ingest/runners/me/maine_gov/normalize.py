#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest.schema import schema  # noqa: E402

CITY_RE = re.compile(r"^([\w ]+), NY$")
# the providerName field smells like it's being parsed from someplace else,
# a good number of them have leading \u1d42 and/or *, which we want to clean.
# there's a bunch with a city name in them, but no real pattern to it, so
# we'll leave that for now.
NAME_CLEAN_RE = re.compile("^[\u1d42*]+")

logger = logging.getLogger(__name__)


def _deprecated_get_inventory(raw: str) -> Optional[List[schema.Vaccine]]:
    # we've only seen "Pfizer", but no reason not to use the rest of the
    # potentials from `ak/arcgis/normalize.py`
    potentials = {
        "pfizer": schema.Vaccine(vaccine="pfizer_biontech"),
        "moderna": schema.Vaccine(vaccine="moderna"),
        "janssen": schema.Vaccine(vaccine="johnson_johnson_janssen"),
        "jjj": schema.Vaccine(vaccine="johnson_johnson_janssen"),
    }
    try:
        return [potentials[raw.lower()]]
    except KeyError:
        logger.error(f"Unexpected vaccine brand: {raw}")
        return None


def _deprecated_get_source(site_blob: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site_blob,
        fetched_at=timestamp,
        fetched_from_uri="https://am-i-eligible.covid19vaccine.health.ny.gov/api/list-providers",
        id=site_blob["providerId"],
        published_at=site_blob["lastUpdated"],
        source="am_i_eligible_covid19vaccine_gov",
    )


def _deprecated_normalize(site_blob: dict, timestamp: str) -> str:
    """
    sample entry:

    {"providerId": 1013, "providerName": "\u1d42**York College - Health and Physical Education Complex - Queens", "vaccineBrand": "Pfizer", "address": "Jamaica, NY", "availableAppointments": "Y", "isShowable": true, "lastUpdated": "2021-04-23T20:04:24"} # noqa: E501
    """
    name = NAME_CLEAN_RE.sub("", site_blob["providerName"]).strip()
    city = CITY_RE.search(site_blob["address"]).group(1)
    appts_available = True if site_blob["availableAppointments"] == "Y" else False

    normalized = schema.NormalizedLocation(
        id=f"am_i_eligible_covid19vaccine_gov:{site_blob['providerId']}",
        name=name,
        availability=schema.Availability(appointments=appts_available),
        inventory=_get_inventory(site_blob["vaccineBrand"]),
        links=[
            schema.Link(
                authority="am_i_eligible_covid19vaccine_gov", id=site_blob["providerId"]
            ),
        ],
        fetched_at=timestamp,
        published_at=site_blob["lastUpdated"],
        source=_get_source(site_blob, timestamp),
    ).dict()
    normalized["address"] = {"city": city, "state": "NY"}
    return normalized


def _get_name(site: dict) -> str:
    return site["providerName"]


def _get_city(site: dict) -> str:
    return site["city"]


def _get_id(site: dict) -> str:
    name = _get_name(site)
    city = _get_city(site)
    return f"me:maine_gov:{name}:{city}"


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://www.maine.gov/covid19/vaccines/vaccination-sites",
        id=_get_id(site),
        source="me:maine_gov",
    )


def _get_contacts(site: dict):
    ret = []
    for phone_number in site["phoneNumber"]:
        ret.append(schema.Contact(phone=phone_number))
    for website in site["website"]:
        ret.append(schema.Contact(website=website))
        ret.append(schema.Contact(other=site["schedulingInfo"]))
    return ret


def _get_notes(site: dict):
    ret = []
    ret.append("city:" + site["city"])
    ret.append("county:" + site["county"])
    if site["minimumAge"] != "":
        ret.append("minimum_age:" + site["minimumAge"])
    if site["audience"] != "":
        ret.append("audience:" + site["audience"])
    if site["schedulingInfo"] != "":
        ret.append("info:" + site["schedulingInfo"])
    return ret


def normalize(site: dict, timestamp: str) -> str:
    normalized = schema.NormalizedLocation(
        id=_get_id(site),
        name=_get_name(site),
        contact=_get_contacts(site),
        source=_get_source(site, timestamp),
        notes=_get_notes(site),
    ).dict()
    return normalized


def normalize_from_list(sites, timestamp: str):
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
            site_blob = json.loads(line)

            normalized_sites = []
            if isinstance(site_blob, list):
                normalized_sites = normalize_from_list(site_blob, parsed_at_timestamp)
            else:
                normalized_sites = normalize_from_list([site_blob], parsed_at_timestamp)

            for normalized_site in normalized_sites:
                json.dump(normalized_site, fout)
                fout.write("\n")
