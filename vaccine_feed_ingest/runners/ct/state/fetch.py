#!/usr/bin/env python

import json
import pathlib
import sys

import requests
from bs4 import BeautifulSoup

URL = "https://www.211ct.org/search"


def _get_csrf_token(session):
    resp = session.get(URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    (meta,) = soup.find_all("meta", attrs={"name": "csrf-token"})
    return meta["content"]


output_dir = pathlib.Path(sys.argv[1])


session = requests.Session()  # we need a session cookie for search to work
csrf_token = _get_csrf_token(session)
page = 1
num_results = 0
done = False
while not done:
    body = {
        "coords": {"lat": 41.6032207, "lng": -73.087749},
        "defaultLocation": "Connecticut",
        "location": "Connecticut",
        "page": page,
        "per_page": "50",
        "service_area": "connecticut",
        "taxonomy_code": ["11172"],
        "tp": ["8917"],
    }

    resp = session.post(URL, json=body, headers={"X-CSRF-Token": csrf_token})
    output_file = output_dir / f"data.raw.{page}.json"
    with output_file.open("w") as fout:
        resp.raise_for_status()
        json.dump(resp.json(), fout)
    num_results += len(resp.json()["results"])
    if num_results >= resp.json()["total_results"]:
        done = True
    else:
        page += 1
