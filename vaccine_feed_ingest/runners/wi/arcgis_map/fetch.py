#!/usr/bin/env python3

from arcgis.features import FeatureLayer

url = "https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_Vaccine_Provider_Sites/MapServer/0"

layer = FeatureLayer(url)
results = layer.query(return_all_records=True)
results.save('/tmp', 'wi_arcgis_map.json')
