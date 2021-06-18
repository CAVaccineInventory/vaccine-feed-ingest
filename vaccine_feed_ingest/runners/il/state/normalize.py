#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import sys
from typing import List, Optional, OrderedDict

import us
import usaddress
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import (
    normalize_address,
    normalize_phone,
    normalize_zip,
    parse_address,
    provider_id_from_name
)

logger = getLogger(__file__)


SOURCE_NAME = "il_state"


def _get_id(site: dict) -> str:
    return provider_id_from_name(site["name"]) or "unknown"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
  
    # there are multiple urls, vaccine, agency, health dept. etc
    if website := site.get("website"):
        return [schema.Contact(website=website)]

    return None


def try_lookup(mapping, value, default, name=None):
    if value is None:
        return default

    try:
        return mapping[value]
    except KeyError as e:
        name = " for " + name or ""
        logger.warn("value not present in lookup table%s: %s", name, e)

        return default


def try_get_list(lis, index, default=None):
    if lis is None:
        return default

    try:
        value = lis[index]
        if value == "none":
            logger.warn("saw none value")
        return value
    except IndexError:
        return default


def _get_lat_long(site):
    return schema.LatLng(
        latitude=site.get("lat"),
        longitude=site.get("long"),
    )


def _get_address(site):
    return schema.Address(
        street1=site.get("address"),
        zip=site.get("zip"),
        state="IL",
        city=site.get("city")
    )


# {"name": "ADAMS COUNTY HEALTH DEPARTMENT", "loc_type": "Public health provider - public health clinic", "address": "330 VERMONT ST", "city": "QUINCY", "zip": "62301", "county": "ADAMS", "website": "https://www.co.adams.il.us/government/departments/health-department/covid-19-vaccination-information", "lat": "39.9338798522949", "long": "-91.4105606079102", "pindetails": "<strong>Public health provider - public health clinic</strong><br/><a href='https://www.co.adams.il.us/government/departments/health-department/covid-19-vaccination-information'>ADAMS COUNTY HEALTH DEPARTMENT</a><br/>330 VERMONT ST<br/>QUINCY, IL  62301"}


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site.get("name"),
        address=_get_address(site),
        location=_get_lat_long(site),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=None,
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,  # TODO
        notes=None,
        active=None,
        source=schema.Source(
            source=SOURCE_NAME,
            id=_get_id(site),
            fetched_from_uri="https://coronavirus.illinois.gov/content/dam/soi/en/web/coronavirus/documents/vaccination-locations.csv",  # noqa: E501
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

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for site_json in fin:
                parsed_site = json.loads(site_json)

                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
