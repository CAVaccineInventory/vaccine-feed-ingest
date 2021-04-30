#!/usr/bin/env python3

import json
import logging
from os.path import join
from typing import Optional, Sequence

import urllib3
from arcgis import GIS

http = urllib3.PoolManager()

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("arcgis")


def fetch_geojson(
    service_item_id: str,
    output_dir: str,
    selected_layers: Optional[Sequence[str]] = None,
) -> None:
    """Save selected layers of the arcgis service item"""
    gis = GIS()
    item = gis.content.get(service_item_id)

    if selected_layers is not None:
        suggest_changing_selected_layers(
            service_item_id,
            [layer.properties.name for layer in item.layers],
            selected_layers,
        )

    for layer in item.layers:
        if selected_layers is not None:
            if layer.properties.name not in selected_layers:
                continue

        results = layer.query(return_all_records=True, out_sr=4326)
        layer_id = layer.properties.id
        file_name = f"{service_item_id}_{layer_id}.json"
        print(f"Saving {layer.properties.name} layer to {file_name}")
        results.save(output_dir, file_name)


def suggest_changing_selected_layers(
    service_item_id: str,
    found_layers: Sequence[str],
    selected_layers: Sequence[str],
) -> None:
    """
    Utility logging:
    * Warn if unavailable layers are requested.
    * Inform if available layers are not requested.
    """
    found_set = set(found_layers)
    selected_set = set(selected_layers)

    extra_layers = selected_set - found_set
    missed_layers = found_set - selected_set

    if len(extra_layers) > 0:
        logger.warn(
            "%s - requested layers which do not exist - %s",
            service_item_id,
            extra_layers,
        )

    if len(missed_layers) > 0:
        logger.info(
            "%s - additional layers available but not selected - %s",
            service_item_id,
            missed_layers,
        )


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


def get_results(
    query_url: str, offset: int, batch_size: int, output_dir: str, format: str
) -> None:
    """Fetch one batch of ArcGIS features from the query_url"""

    # Set Output Spatial reference to EPSG 4326 GPS coords
    out_sr = "4326"

    r = http.request(
        "GET",
        query_url,
        fields={
            "where": "1=1",
            "outSR": out_sr,
            "f": format,
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


def fetch(
    query_url: str, output_dir: str, batch_size: int = 50, format: str = "geojson"
) -> None:
    """Fetch ArcGIS features in chunks of batch_size"""

    count = get_count(query_url)
    print(f"Found {count} results")

    for offset in range(0, count, batch_size):
        get_results(query_url, offset, batch_size, output_dir, format)
