#!/usr/bin/env python

import datetime
import json
import pathlib
import re
import sys
import urllib.parse
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


def _get_id(site: dict) -> str:
    data_id = site["id"]

    name = site["title"]
    city = site["location"]["city"]

    id = f"{name}_{city}_{data_id}"
    id = re.sub(r"[^a-zA-Z0-9_]", "", id.lower())
    return id


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    records_with_bad_urls = [
        "38327",
        "38316",
        "38328",
        "38465",
        "38852",
        "39047",
        "39048",
        "39072",
        "39519",
        "39520",
        "40071",
    ]

    if site["id"] not in records_with_bad_urls and site["location"]["extra_fields"].get(
        "website", None
    ):
        url = site["location"]["extra_fields"]["website"]
        if not url.startswith("http://") and not url.startswith("https://"):
            # stuff a scheme at the beginning if one is missing or
            # typo-ed
            url = re.sub(r"^(.*?:(//)?)?", "http://", url)

        # some URLs have spaces in them
        url = urllib.parse.quote(url, safe=":/")

        contacts.append(schema.Contact(website=url))

    if len(contacts) > 0:
        return contacts

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    if site["location"]["extra_fields"]["additional-information"]:
        return [site["location"]["extra_fields"]["additional-information"]]

    return None


def convert_lat_lng(val: str) -> float:
    # some lat/lng values have a spurious comma at the end
    return float(re.sub(r",$", "", val))


def normalize(site: dict, timestamp: str) -> schema.NormalizedLocation:
    source_name = "fl_state"
    return schema.NormalizedLocation(
        id=f"{source_name}:{_get_id(site)}",
        name=site["title"],
        address=schema.Address(
            street1=site["address"],
            street2=None,
            city=site["location"]["city"],
            state="FL",
            zip=site["location"].get("postal_code", None),
        ),
        location=schema.LatLng(
            latitude=convert_lat_lng(site["location"]["lat"]),
            longitude=convert_lat_lng(site["location"]["lng"]),
        ),
        contact=_get_contacts(site),
        notes=_get_notes(site),
        source=schema.Source(
            source=source_name,
            id=site["id"],
            fetched_from_uri="https://floridahealthcovid19.gov/vaccines/vaccine-locator/",
            fetched_at=timestamp,
            data=site,
        ),
    )


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

            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
