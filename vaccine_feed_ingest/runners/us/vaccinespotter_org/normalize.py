#!/usr/bin/env python
# isort: skip_file

import datetime
import json
from vaccine_feed_ingest.utils.log import getLogger
import pathlib
import sys
from typing import Optional

import pydantic
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.normalize import provider_id_from_name
from vaccine_feed_ingest.utils.validation import BOUNDING_BOX

logger = getLogger(__file__)


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


def _get_contact(site) -> Optional[schema.Contact]:

    if url := site.get("url"):
        return [
            schema.Contact(contact_type=None, phone=None, website=url),
        ]
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


def _strip_source_data(site_blob: dict) -> None:
    """Strip out fields which make the source blob too big to store"""
    # Remove list of all available appoitments times. We are not using this information
    # and it makes each record 65K+
    if site_blob.get("properties") and site_blob["properties"].get("appointments"):
        del site_blob["properties"]["appointments"]


def normalize(site_blob: dict, timestamp: str) -> dict:
    site = site_blob["properties"]

    links = [schema.Link(authority="vaccinespotter_org", id=site["id"])]
    brandname = site["provider_brand_name"]
    parsed_provider_link = (
        provider_id_from_name(brandname) if brandname else None  # or use site["name"]?
    )
    if parsed_provider_link is not None:
        links.append(
            schema.Link(authority=parsed_provider_link[0], id=parsed_provider_link[1])
        )

    _strip_source_data(site_blob)

    return schema.NormalizedLocation(
        id=f"vaccinespotter_org:{site['id']}",
        name=site["name"],
        address=_get_address(site),
        location=_get_lat_lng(site_blob["geometry"], site["id"]),
        contact=_get_contact(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=schema.Availability(
            appointments=site["appointments_available"], drop_in=None
        ),
        inventory=None,
        access=schema.Access(walk=None, drive=None, wheelchair=None),
        parent_organization=None,
        links=links,
        notes=None,
        active=None,
        source=schema.Source(
            source="vaccinespotter_org",
            id=site["id"],
            fetched_from_uri="https://www.vaccinespotter.org/api/v0/US.json",  # noqa: E501
            fetched_at=timestamp,
            published_at=site["appointments_last_fetched"],
            data=site_blob,
        ),
    ).dict()


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
