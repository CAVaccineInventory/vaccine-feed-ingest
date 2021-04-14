#!/usr/bin/env python3

import json
import urllib3
from os.path import join

http = urllib3.PoolManager()


def get_count(query_url: str) -> int:
    """
    Get the total count of features in this ArcGIS feed.
    This will be used for querying results in batches.
    """

    r = http.request(
        "GET",
        query_url,
        fields={"where": "1=1", "returnCountOnly": "true", "f": "json"},
    )
    obj = json.loads(r.data.decode("utf-8"))
    return obj["count"]


def get_results(query_url: str, offset: int, batch_size: int, output_dir: str):
    """ Fetch one batch of ArcGIS features from the query_url """

    # Set Output Spatial reference to EPSG 4326 GPS coords
    out_sr = "4326"

    r = http.request(
        "GET",
        query_url,
        fields={
            "where": "1=1",
            "outSR": out_sr,
            "f": "json",
            "outFields": "*",
            "returnGeometry": "true",
            "orderByFields": "objectId ASC",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
        },
    )

    # obj = json.loads(r.data.decode('utf-8'))

    output_file = join(output_dir, f"{offset}.json")
    with open(output_file, "wb") as fh:
        print(f"Writing {output_file}")
        fh.write(r.data)


def fetch(query_url: str, output_dir: str, batch_size=50):
    """ Fetch ArcGIS features in chunks of batch_size """

    count = get_count(query_url)
    print(f"Found {count} results")

    for offset in range(0, count, batch_size):
        get_results(query_url, offset, batch_size, output_dir)
