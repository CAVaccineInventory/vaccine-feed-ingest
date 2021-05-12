#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional

import pyproj
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["OBJECTID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis_map"
    runner = "wi"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "wi_arcgis_map"
    layer = 0

    return f"{runner}_{site_name}:{arcgis}_{layer}_{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    if website := site["attributes"]["WEBSITE_URL"]:
        if re.match(r"^https://(www\.)?google\.com/search\?q=", website):
            website = website.replace(" ", "+")

        if website == "https://wildwoodfamilyclinic/":
            website = "https://wp.wildwoodclinic.com/"

        # workaround until samuelcolvin/pydantic#2778 is merged
        website = website.rstrip("#")

        return [schema.Contact(website=website)]

    return None


transformer = pyproj.Transformer.from_crs(3857, 4326)


def _get_location(site: dict) -> Optional[schema.LatLng]:
    if site["geometry"]["x"] == "NaN":
        return None

    x, y = transformer.transform(site["geometry"]["x"], site["geometry"]["y"])
    return schema.LatLng(
        latitude=x,
        longitude=y,
    )


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["NAME"],
        address=schema.Address(
            street1=site["attributes"]["ADDRESS"],
            street2=None,
            city=site["attributes"]["CITY"],
            state=site["attributes"]["STATE"],
            zip=site["attributes"]["ZIP"],
        ),
        location=_get_location(site),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=None,
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=None,
        source=schema.Source(
            source="wi_arcgis_map",
            id=site["attributes"]["OBJECTID"],
            fetched_from_uri="https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_Vaccine_Provider_Sites/MapServer/0",  # noqa: E501
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
