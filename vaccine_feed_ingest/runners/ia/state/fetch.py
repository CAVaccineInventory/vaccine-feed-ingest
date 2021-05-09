#!/usr/bin/env python3
import json
import os
import sys

import requests

SEARCH_ENDPOINT = "https://vaccinate.iowa.gov/providers/SearchProviders"

# IA is much less than 1000 miles across, so these param should
# be more than sufficient
SEARCH_RADIUS = 1000
CENTRAL_ZIP_CODE = 50010


def main(argv):
    output_dir = argv[1]
    outfile_path = os.path.join(output_dir, "ia_state.json")
    response = requests.post(
        SEARCH_ENDPOINT, {"address": CENTRAL_ZIP_CODE, "range": SEARCH_RADIUS}
    )
    if not response.ok:
        response.raise_for_status()
    content = response.json()
    if not content["Success"]:
        sys.exit(f"Request Failed: {content['Message']}")

    with open(outfile_path, "w") as outfile:
        json.dump(content, outfile)


if __name__ == "__main__":
    main(sys.argv)
