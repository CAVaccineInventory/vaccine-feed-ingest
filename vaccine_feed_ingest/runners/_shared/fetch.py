#!/usr/bin/env python3

import pathlib
import sys

import yaml

from vaccine_feed_ingest.utils.log import getLogger

# import arcgis ingestor
shared_dir = pathlib.Path(__file__).parent
runner_dir = shared_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))

from ingestors import arcgis_ingest  # noqa: E402

logger = getLogger(__file__)

output_dir = sys.argv[1]
yml_config = sys.argv[2]

config = None
with open(yml_config, "r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

try:
    state = config["state"]
except KeyError as e:
    logger.error(
        "config file must have key 'state'. This config does not - %s", yml_config
    )
    raise e

try:
    logger.info(
        "Scraping %s/arcgis into output_dir=%s, with config from %s",
        state.upper(),
        output_dir,
        yml_config,
    )
    for service_item in config["arcgis"]:
        if len(service_item["layer_names"]) > 0:
            arcgis_ingest.fetch_geojson(
                service_item["id"], output_dir, service_item["layer_names"]
            )
except KeyError as e:
    logger.error(
        "config file must have key 'arcgis' containing a list of objects, "
        "each with a key 'id' and a key 'layer_names'. This config does not - %s",
        yml_config,
    )
    raise e
