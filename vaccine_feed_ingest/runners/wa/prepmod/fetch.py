#!/usr/bin/env python3

import os
import sys

import requests

base_url = "https://prepmod.doh.wa.gov/clinic/search"

output_dir = sys.argv[1]
if output_dir is None:
    print("Must pass an output_dir as first argument")

page = 1
while True:
    url = "{}?page={}".format(base_url, page)
    response = requests.get(url, allow_redirects=False)

    # when out of results, will return 302
    if response.status_code != 200:
        break

    with open(os.path.join(output_dir, "{}.html".format(page)), "w") as f:
        f.write(response.text)

    page += 1
