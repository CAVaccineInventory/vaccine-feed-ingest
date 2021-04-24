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


def _get_inventory(raw: str) -> Optional[List[schema.Vaccine]]:
    # we've only seen "Pfizer", but no reason not to use the rest of the
    # potentials from `ak/arcgis/normalize.py`
    potentials = {
        "pfizer": schema.Vaccine(vaccine="pfizer"),
        "moderna": schema.Vaccine(vaccine="moderna"),
        "janssen": schema.Vaccine(vaccine="janssen"),
        "jjj": schema.Vaccine(vaccine="janssen"),
    }
    try:
        return [potentials[raw.lower()]]
    except KeyError:
        logger.error(f"Unexpected vaccine brand: {raw}")
        return None


def _get_source(site_blob: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site_blob,
        fetched_at=timestamp,
        fetched_from_uri="https://am-i-eligible.covid19vaccine.health.ny.gov/api/list-providers",
        id=site_blob["providerId"],
        published_at=site_blob["lastUpdated"],
        source="am_i_eligible_covid19vaccine_gov",
    )


def normalize(site_blob: dict, timestamp: str) -> str:
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


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)
            normalized_site = normalize(site_blob, parsed_at_timestamp)
            json.dump(normalized_site, fout)
            fout.write("\n")
