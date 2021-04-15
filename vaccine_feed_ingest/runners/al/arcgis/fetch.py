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
print(f"Starting AL scraper with {output_dir=}")

# Ingest layers from Alabama ArgGIS dashboard:
# https://alpublichealth.maps.arcgis.com/apps/opsdashboard/index.html#/2b4627aa70c5450791a7cf439ed047ec

# Vaccination_Dashboard_AGOL_v4_PUBLIC_VIEW
arcgis.fetch_geojson("51d4c310f1fe4d83a63e2b47acb77898", output_dir, ["Providers"])

# FederalPartners_PUBLIC_VIEW
arcgis.fetch_geojson(
    "8f23e1c3b5c54198ab60d2f729cb787d", output_dir, ["FederalPartners"]
)

# ApptOnly2_Public
arcgis.fetch_geojson("d1a799c7f98e41fb8c6b4386ca6fe014", output_dir, ["ApptOnly2"])

# DriveThruWalkIn2_Public
arcgis.fetch_geojson("8537322b652841b4a36b7ddb7bc3b204", output_dir, ["DriveThruWalkIn"])
