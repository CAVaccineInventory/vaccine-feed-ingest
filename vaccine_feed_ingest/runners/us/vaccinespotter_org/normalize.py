#!/usr/bin/env python
# isort: skip_file

import datetime
import json
import logging
import pathlib
import sys
from typing import Optional

import pydantic
from vaccine_feed_ingest_schema import schema

from vaccine_feed_ingest.utils.normalize import provider_id_from_name
from vaccine_feed_ingest.utils.validation import BOUNDING_BOX

logger = logging.getLogger("us/vaccinespotter_org")


def _get_address(site: dict) -> Optional[schema.Address]:
    try:
        return schema.Address(
            street1=site["address"],
            city=site["city"],
            state=site["state"],
            zip=site["postal_code"],
        )
    except pydantic.ValidationError:
        logger.warning(
            "Invalid address for %s (%s). Returning None",
            site["id"],
            f"{site['address']} {site['city']} {site['state']} {site['postal_code']}",
        )

    return None


def _get_lat_lng(geometry: dict, id: str) -> Optional[schema.LatLng]:
    try:
        lat_lng = schema.LatLng(
            latitude=geometry["coordinates"][1], longitude=geometry["coordinates"][0]
        )
        if BOUNDING_BOX.latitude.contains(
            lat_lng.latitude
        ) and BOUNDING_BOX.longitude.contains(lat_lng.longitude):
            return lat_lng

        logger.warning(
            "Out of bounds lat/lng for %s (%s). Returning None",
            id,
            f"lat={geometry['coordinates'][1]}, lng={geometry['coordinates'][0]}",
        )
    except pydantic.ValidationError:
        logger.warning(
            "Invalid lat/lng for %s (%s). Returning None",
            id,
            f"lat={geometry['coordinates'][1]}, lng={geometry['coordinates'][0]}",
        )

    return None


def normalize(site_blob: dict, timestamp: str) -> dict:
    site = site_blob["properties"]

    address = _get_address(site)
    if address:
        address = address.dict()

    lat_lng = _get_lat_lng(site_blob["geometry"], site["id"])
    if lat_lng:
        lat_lng = lat_lng.dict()

    normalized = {
        "id": f"vaccinespotter_org:{site['id']}",
        "name": site["name"],
        "address": address,
        "location": lat_lng,
        "contact": [
            {
                "contact_type": None,
                "phone": None,
                "website": site["url"],
                "other": None,
            },
        ],
        "availability": {
            "appointments": site["appointments_available"],
            "drop_in": None,
        },
        "access": {
            "walk": None,
            "drive": None,
            "wheelchair": None,
        },
        "languages": [],
        "links": [
            {
                "authority": "vaccinespotter_org",
                "id": site["id"],
            },
        ],
        "fetched_at": timestamp,
        "published_at": site[
            "appointments_last_fetched"  # we could also use `appointments_last_modified`
        ],
        "active": None,
        "source": {
            "source": "vaccinespotter_org",
            "id": site["id"],
            "fetched_from_uri": "https://www.vaccinespotter.org/api/v0/US.json",
            "fetched_at": timestamp,
            "published_at": site[
                "appointments_last_fetched"  # we could also use `appointments_last_modified`
            ],
            "data": site_blob,
        },
    }

    parsed_provider_link = provider_id_from_name(
        site["provider_brand_name"]  # or use site["name"]?
    )
    if parsed_provider_link is not None:
        normalized["links"].append(
            {"authority": parsed_provider_link[0], "id": parsed_provider_link[1]}
        )
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
