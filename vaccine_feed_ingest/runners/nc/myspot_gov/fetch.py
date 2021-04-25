import json
import os
import sys

import requests

output_directory = sys.argv[1]

nc_vaccine_info_website_api = "https://myspot.nc.gov/api/get-vaccine-locations"
nc_vaccine_data = requests.get(nc_vaccine_info_website_api).json()
nc_vaccine_data_output_filename = "nc_data.json"

with open(
    os.path.join(output_directory, nc_vaccine_data_output_filename), "w"
) as outfile:
    json.dump(nc_vaccine_data, outfile)
