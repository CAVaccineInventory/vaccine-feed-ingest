#!/usr/bin/env python

import datetime
import json
import pathlib
import sys

from vaccine_feed_ingest_schema import schema  # noqa: E402

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

_source_name = "ca_metrolink"


def _get_id(site: dict) -> str:
    name = _get_name(site)
    city = _get_city(site)

    id = f"{name}_{city}".lower()
    id = id.replace(" ", "_").replace("\u2019", "_").replace("#", "_")
    id = id.replace(".", "_").replace(",", "_").replace("'", "_")
    id = id.replace("(", "_").replace(")", "_").replace("/", "_")

    return id


def _get_name(site: dict) -> str:
    return site["name"]


def _get_city(site: dict) -> str:
    return site["city"]


def _get_address(site: dict):
    return schema.Address(
        street1=site["address"],
        city=_get_city(site),
        zip=None,
        state="CA",
    )


def _get_notes(site: dict):
    ret = []
    metrolink_station = site["metrolink_station"]
    metrolink_line = site["metrolink_line"]
    ret.append(
        f"ca_metrolink_directions: Near the {metrolink_station} station on the {metrolink_line} line."
    )
    return ret


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://register.metrolinktrains.com/web-assets/vax-sites/index.php",
        id=_get_id(site),
        source=_source_name,
    )


def normalize(site: dict, timestamp: str) -> str:
    normalized = schema.NormalizedLocation(
        id=(_source_name + ":" + _get_id(site)),
        name=_get_name(site),
        address=_get_address(site),
        notes=_get_notes(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "metrolink.html.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "metrolink.html.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)

            normalized_site = normalize(site_blob, parsed_at_timestamp)

            json.dump(normalized_site, fout)
            fout.write("\n")
