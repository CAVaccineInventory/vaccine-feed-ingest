#!/usr/bin/env python3

import json
import os
import requests
import sys

if len(sys.argv) < 2:
    print("Must pass an output_dir as first argument")
    sys.exit(1)
output_dir = sys.argv[1]


def call_api(output_filename, method, params={}):
    url = "https://coronavirus.illinois.gov/s/sfsites/aura?aura.ApexAction.execute=1"

    message = {
        "actions": [
            {
                "id": "1;a",
                "descriptor": "aura://ApexActionController/ACTION$execute",
                "callingDescriptor": "UNKNOWN",
                "params": {
                    "namespace": "",
                    "classname": "VaccinationSitesApex",
                    "method": method,
                    "params": params,
                    "cacheable": False,
                    "isContinuation": False,
                },
            }
        ]
    }

    context = {
        "mode": "PROD",
        "fwuid": "Q8onN6EmJyGRC51_NSPc2A",
        "app": "siteforce:communityApp",
        "loaded": {
            "APPLICATION@markup://siteforce:communityApp": "Lxj49OM4CA4D42prjs-b3A",
        },
        "dn": [],
        "globals": {},
        "uad": False,
    }

    response = requests.post(
        url,
        data={
            "message": json.dumps(message),
            "aura.context": json.dumps(context),
            "aura.token": "undefined",
        },
    )

    response.raise_for_status()
    with open(os.path.join(output_dir, output_filename), "w") as f:
        f.write(response.text)


call_api("statewide.json", "getMassVaccSites")
call_api("locations.json", "getSites", {"countOfMiles": "10", "selectedCity": None})
