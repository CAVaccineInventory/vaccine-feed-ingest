#!/usr/bin/env python3

import json
import os
import sys

import requests

base_url = "https://api.northwell.edu/"
url = "https://api.northwell.edu/v2/vax-locations/all"


def get_paginated_urls():
    response = requests.get(url)
    data = response.json()
    return [page_url["url"] for page_url in data["response"]["pagination"]["display"]]


def get_locations(page_url):
    response = requests.get(base_url + page_url)
    data = response.json()
    return data["response"]["locations"]


def main():
    output_dir = sys.argv[1]
    if output_dir is None:
        raise Exception("Must pass an output_dir as first argument")

    page_urls = get_paginated_urls()
    for index, page_url in enumerate(page_urls):
        locations = get_locations(page_url)
        output_file_path = os.path.join(output_dir, f"output{index}.json")
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(locations, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    sys.exit(main())
