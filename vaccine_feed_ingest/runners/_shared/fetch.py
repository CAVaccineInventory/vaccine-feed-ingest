#!/usr/bin/env python3

import os
import pathlib
import sys
import urllib.parse

import requests
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
        logger.error(exc)

try:
    state = config["state"]
except KeyError as e:
    logger.error(
        "config file must have key 'state'. This config does not - %s", yml_config
    )
    raise e

logger.info(
    "Scraping %s/%s into output_dir=%s, with config from %s",
    state.upper(),
    config.get("parser", "arcgis"),
    output_dir,
    yml_config,
)

if "parser" not in config or config["parser"] == "arcgis":
    try:
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
elif config["parser"] == "prepmod":
    try:
        base_url = urllib.parse.urljoin(config["url"], "appointment/en/clinic/search")
        page = 1
        while True:
            params = urllib.parse.urlencode({"page": page})
            url = f"{base_url}?{params}"
            response = requests.get(url, allow_redirects=False)

            # when out of results, will return 302
            if response.status_code != 200:
                break

            with open(os.path.join(output_dir, f"{page}.html"), "w") as f:
                f.write(response.text)

            page += 1
    except KeyError as e:
        logger.error(
            "config file must have key 'url'. This config does not - %s",
            yml_config,
        )
        raise e
else:
    logger.error("Parser '%s' was not recognized.", config["parser"])
    raise NotImplementedError(f"No shared parser available for '{config['parser']}'.")
