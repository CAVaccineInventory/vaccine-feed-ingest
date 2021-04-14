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

arcgis.fetch_geojson("51d4c310f1fe4d83a63e2b47acb77898", output_dir, ["Providers"])
