#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional

from arcgis.geometry import project
from arcgis.gis import GIS
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

GIS()  # initialize GIS, for _get_locations_batch


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


# The input geometry is in a different projection from our normalized geometry,
# so we need to project it. The projection api from arcgis has a noticeable
# per-call overhead, so we batch the projections.
def _get_locations_batch(sites: List[dict]) -> List[Optional[schema.LatLng]]:
    result = []
    for x in project([s["geometry"] for s in sites], in_sr=3857, out_sr=4326):
        if "coordinates" in x:
            coords = x["coordinates"]
            result.append(
                schema.LatLng(
                    latitude=coords[1],
                    longitude=coords[0],
                )
            )
        else:
            result.append(None)
    return result


def _get_normalized_location(
    site: dict, timestamp: str, location: Optional[schema.LatLng]
) -> schema.NormalizedLocation:
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
        location=location,
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
            sites = [json.loads(s) for s in fin]
            locations = _get_locations_batch(sites)
            for parsed_site, location in zip(sites, locations):
                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp, location
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
