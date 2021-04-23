#!/usr/bin/env python3

import json
import logging
import os
import pathlib
import sys

import yaml

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("_shared/parse.py")

OUTPUT_DIR = pathlib.Path(sys.argv[1])
INPUT_DIR = pathlib.Path(sys.argv[2])
YML_CONFIG = pathlib.Path(sys.argv[3])

with open(YML_CONFIG, "r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

try:
    state = config["state"]
except KeyError as e:
    logger.error(
        "config file must have key 'state'. This config does not - %s", YML_CONFIG
    )
    raise e

try:
    site = config["site"]
except KeyError as e:
    logger.error(
        "config file must have key 'site'. This config does not - %s", YML_CONFIG
    )
    raise e

try:
    parser = config["parser"]
except KeyError as e:
    logger.error(
        "config file must have key 'parser'. This config does not - %s", YML_CONFIG
    )
    raise e

if parser == "arcgis_features":
    json_filepaths = INPUT_DIR.glob("*.json")

    for in_filepath in json_filepaths:
        with in_filepath.open() as fin:
            arcgis_feature_json = json.load(fin)

        filename, _ = os.path.splitext(in_filepath.name)
        out_filepath = OUTPUT_DIR.joinpath(f"{filename}.parsed.ndjson")

        logger.info(
            "(%s/%s) parsing %s => %s",
            state.upper(),
            site.lower(),
            in_filepath,
            out_filepath,
        )
        with out_filepath.open("w") as fout:
            for feature in arcgis_feature_json["features"]:
                json.dump(feature, fout)
                fout.write("\n")
else:
    logger.error("Parser '%s' was not recognized.", parser)
    raise NotImplementedError(
        f"No shared parser available for '{parser}'."
    )
