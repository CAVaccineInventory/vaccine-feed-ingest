#!/usr/bin/env python3

import json
from os.path import join
from typing import Optional, Sequence

import urllib3
from arcgis import GIS

http = urllib3.PoolManager()


def fetch_geojson(
    service_item_id: str,
    output_dir: str,
    selected_layers: Optional[Sequence[str]] = None,
):
    """ Save selected layers of the arcgis service item """
    gis = GIS()
    item = gis.content.get(service_item_id)
    for layer in item.layers:
        if selected_layers is not None:
            if layer.properties.name not in selected_layers:
                continue

        results = layer.query()
        layer_id = layer.properties.id
        file_name = f"{service_item_id}_{layer_id}.json"
        print(f"Saving {layer.properties.name} layer to {file_name}")
        results.save(output_dir, file_name)


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


def get_results(query_url: str, offset: int, batch_size: int, output_dir: str) -> None:
    """ Fetch one batch of ArcGIS features from the query_url """

    # Set Output Spatial reference to EPSG 4326 GPS coords
    out_sr = "4326"

    r = http.request(
        "GET",
        query_url,
        fields={
            "where": "1=1",
            "outSR": out_sr,
            "f": "pgeojson",
            "outFields": "*",
            "returnGeometry": "true",
            "orderByFields": "objectId ASC",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
        },
    )

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
