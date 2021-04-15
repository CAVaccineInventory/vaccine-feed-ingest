#!/usr/bin/env python3

import pathlib
import sys

# import arcgis ingestor
site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))
from ingestors import arcgis  # noqa: E402

output_dir = sys.argv[1]
print(f"Starting MD scraper with {output_dir=}")

# Ingest layers from Maryland ArgGIS dashboard:
# https://maryland.maps.arcgis.com/apps/instant/nearby/index.html?appid=0dbfb100676346ed9758be319ab3f40c

arcgis.fetch_geojson(
    "d677f143334648a1a40b84d94df8e134",
    output_dir,
    [
        "Mass Vaccination Sites",
        "Pharmacy Vaccination Sites",
        "Local Health Department Vaccination Sites",
        "Hospital Vaccination Sites",
        "All Maryland Vaccination Sites",
    ],
)
