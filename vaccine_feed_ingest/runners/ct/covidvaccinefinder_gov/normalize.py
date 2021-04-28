#!/usr/bin/env python
# isort: skip_file

import datetime
import json
import logging
import pathlib
import sys
from typing import Optional

import pydantic
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.normalize import provider_id_from_name
from vaccine_feed_ingest.utils.validation import BOUNDING_BOX

logger = logging.getLogger("ct/covidvaccinefinder_gov")


def _in_bounds(lat_lng: schema.LatLng) -> bool:
    if BOUNDING_BOX.latitude.contains(
        lat_lng.latitude
    ) and BOUNDING_BOX.longitude.contains(lat_lng.longitude):
        return True
    return False


def _get_lat_lng(site: dict) -> Optional[schema.LatLng]:
    try:
        source_lat_lng = schema.LatLng(latitude=site["lat"], longitude=site["lng"])

        # In the CT data source, some lat/lng pairs are flipped.
        # If the lat/lng from the datasource is outside our expected boundaries,
        # flip them.
        if not _in_bounds(source_lat_lng):
            flipped_lat_lng = schema.LatLng(
                latitude=source_lat_lng.longitude, longitude=source_lat_lng.latitude
            )
            if not _in_bounds(flipped_lat_lng):
                logger.warning(
                    "Out of bounds and unflippable lat/lng for %s (%s)",
                    site["_id"],
                    source_lat_lng,
                )
                return None
            return flipped_lat_lng
        return source_lat_lng

    except pydantic.ValidationError as e:
        logger.warning("Invalid or missing lat/lng for %s: %s", site["_id"], str(e))

    return None


def normalize(site: dict, timestamp: str) -> dict:
    links = [
        schema.Link(authority="ct_gov", id=site["_id"]),
        schema.Link(authority="ct_gov:network_id", id=site["networkId"])
    ]

    parent_organization = schema.Organization(name=site["networks"][0]["name"])

    parsed_provider_link = provider_id_from_name(site["name"])
    if parsed_provider_link is not None:
        links.append(
            schema.Link(authority=parsed_provider_link[0], id=parsed_provider_link[1])
        )

        parent_organization.id = parsed_provider_link[0]

    return schema.NormalizedLocation(
        id=f"ct_gov:{site['_id']}",
        name=site["displayName"],
        address=schema.Address(
            street1=site["addressLine1"],
            street2=site["addressLine2"],
            city=site["city"],
            state="CT",
            zip=site["zip"],
        ),
        location=_get_lat_lng(site),
        contact=[
            schema.Contact(
                contact_type="booking",
                phone=site["phone"],
                website=site["link"]
                ),
        ],
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=schema.Availability(
            appointments=site["availability"],
            ),
        inventory=[{"vaccine": vaccine["name"]} for vaccine in site["providerVaccines"]],
        access=schema.Access(
            drive=site["isDriveThru"],
            ),
        parent_organization=parent_organization,
        links=links,
        notes=None,
        active=None,
        source=schema.Source(
            source="covidvaccinefinder_gov",
            id=site["_id"],
            fetched_from_uri="https://covidvaccinefinder.ct.gov/api/HttpTriggerGetProvider",  # noqa: E501
            fetched_at=timestamp,
            published_at=site["lastModified"],
            data=site,
        ),
    ).dict()


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

input_file = input_dir / "data.parsed.ndjson"
output_file = output_dir / "data.normalized.ndjson"

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

with input_file.open() as fin:
    with output_file.open("w") as fout:
        for site_json in fin:
            parsed_site = json.loads(site_json)

            normalized_site = normalize(parsed_site, parsed_at_timestamp)

            json.dump(normalized_site, fout)
            fout.write("\n")
