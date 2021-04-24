#!/usr/bin/env python

import datetime
import json
import pathlib
import re
import sys

from vaccine_feed_ingest.utils.normalize import provider_id_from_name

CITY_RE = re.compile("^([\w ]+), NY$")
# the providerName field smells like it's being parsed from someplace else,
# a good number of them have leading \u1d42 and/or *, which we want to clean.
# there's a bunch with a city name in them, but no real pattern to it, so
# we'll leave that for now.
NAME_CLEAN_RE = re.compile("^[\u1d42*]+")


def normalize(site_blob: dict, timestamp: str) -> dict:
    """
    sample entry:

    {"providerId": 1013, "providerName": "\u1d42**York College - Health and Physical Education Complex - Queens", "vaccineBrand": "Pfizer", "address": "Jamaica, NY", "availableAppointments": "Y", "isShowable": true}
    """
    name = NAME_CLEAN_RE.sub("", site_blob["providerName"]).strip()
    city = CITY_RE.search(site_blob["address"]).group(1)
    appts_available = True if site_blob["availableAppointments"] == "Y" else False
    brand = site_blob["vaccineBrand"]

    normalized = {
        "id": f"am_i_eligible_covid19vaccine_gov:{site_blob['providerId']}",
        "name": name,
        "address": {
            "city": city,
            "state": "NY",
        },
        "availability": {
            "appointments": appts_available,
        },
        "inventory": [
            {
                "vaccine": brand,
            },
        ],
        "links": [
            {
                "authority": "am_i_eligible_covid19vaccine_gov",
                "id": site_blob["providerId"],
            },
        ],
        "fetched_at": timestamp,
        "published_at": site_blob["lastUpdated"],
        "source": {
            "data": site_blob,
            "fetched_at": timestamp,
            "fetched_from_uri": "https://am-i-eligible.covid19vaccine.health.ny.gov/api/list-providers",
            "id": site_blob["providerId"],
            "published_at": site_blob["lastUpdated"],
            "source": "am_i_eligible_covid19vaccine_gov",
        },
    }
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
