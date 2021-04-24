#!/usr/bin/env python3

import sys

from arcgis.features import FeatureLayer

url = "https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_Vaccine_Provider_Sites/MapServer/0"


output_dir = sys.argv[1]
if output_dir is None:
    print("Must pass an output_dir as first argument")

layer = FeatureLayer(url)
results = layer.query(return_all_records=True)
results.save(output_dir, "wi_arcgis_map.json")
