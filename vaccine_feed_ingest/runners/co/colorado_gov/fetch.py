#!/usr/bin/env python

import pathlib
import sys

import requests

"""
    This script uses the KML export available from Google maps.
"""

output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "colorado_gov.kml"

# To find this URL
# 1. Open the google map in a browser and open the Network Tab in dev tools
# 2. Open the menu in the header of the google maps viewer, probably 3 vertical dots
# 3. Choose Download KML and in the popup dialog select "Export as KML instead of KMZ. Does not support all icons."
# 4. You will be prompted to save the file somewhere, cancel that as it's not important right now
# 5. Back in the Network Tab you should see a URL like the following:
KML_DOWNLOAD_URL = (
    "https://www.google.com/maps/d/kml?mid=1x9KT3SJub0igOTnhFtdRYmceZuBXMWvK&forcekml=1"
)

r = requests.get(KML_DOWNLOAD_URL)

with open(output_file, "w") as fout:
    fout.write(r.text)
    fout.write("\n")
