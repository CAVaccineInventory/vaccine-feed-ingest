#!/usr/bin/env python
# This scraper is inactive as the data source is no longer being updated.

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

ADDRESS_RE = re.compile(r"(.*)(?:,|\n)([a-zA-Z. ]+),\s?(?:IL|Illinois)?\s+(\d{5})")

# Rewrites for addresses that don't quite fit the regex.
ADDRESS_FIXES = {
    "3401 N Knox Ave Chicago, IL 60641": "3401 N Knox Ave\nChicago, IL 60641",
    "2674 N Halsted St Chicago IL 60614\n": "2674 N Halsted St\nChicago, IL 60614",
    "701 W North Ave, Melrose Park, IL": "701 W North Ave, Melrose Park, IL 60160",
    "2000 5th Ave River Grove, IL 60171": "2000 5th Ave\nRiver Grove, IL 60171",
    "3128 Voyager Lane\nJoliet, IL": "3128 Voyager Lane\nJoliet, IL 60431",
}


def _get_notes(site: dict) -> Optional[List[str]]:
    if site.get("notes"):
        return site["notes"]
    return None


def _get_source_id(site: dict) -> str:
    return site["id"].split(":")[1]


def _normalize_address(site: dict) -> Optional[schema.Address]:
    address = site.get("address")
    if not address:
        logger.warning(f"No address for {site}")
        return None

    # Rewrite slightly-broken addresses.
    if a := ADDRESS_FIXES.get(address):
        address = a

    street1 = None
    city = None
    zip = None
    if m := ADDRESS_RE.search(address):
        street1 = m.group(1)
        city = m.group(2)
        zip = m.group(3)
    else:
        logger.warning(f"No address match found for {site}")

    return schema.Address(
        street1=street1,
        street2=None,
        city=city,
        state="IL",
        zip=zip,
    )


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=site["id"],
        name=site["name"],
        address=_normalize_address(site),
        contact=site["contact"],
        notes=_get_notes(site),
        source=schema.Source(
            source="ilvaccine_org",
            id=_get_source_id(site),
            fetched_from_uri="https://ilvaccine-api.us-east-1.linodeobjects.com/vts.ndjson",
            fetched_at=timestamp,
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