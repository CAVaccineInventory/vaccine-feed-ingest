#!/usr/bin/env python3

import json
import os
import pathlib
import sys
from typing import List

import yaml

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

OUTPUT_DIR = pathlib.Path(sys.argv[1])
INPUT_DIR = pathlib.Path(sys.argv[2])
YML_CONFIG = pathlib.Path(sys.argv[3])


def _enforce_keys(config: dict, keys: List[str]) -> None:
    for key in keys:
        if key not in config:
            logger.error("config file must have key 'state'. This config does not.")
            raise KeyError(f"{key} not found")


def _get_config(yml_config: pathlib.Path) -> dict:
    with open(yml_config, "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    _enforce_keys(config, ["state", "site", "parser"])

    return config


def _get_out_filepath(in_filepath: pathlib.Path, out_dir: pathlib.Path) -> pathlib.Path:
    filename, _ = os.path.splitext(in_filepath.name)
    return out_dir.joinpath(f"{filename}.parsed.ndjson")


def _log_activity(
    state: str, site: str, in_filepath: pathlib.Path, out_filepath: pathlib.Path
) -> None:
    logger.info(
        "(%s/%s) parsing %s => %s",
        state.upper(),
        site.lower(),
        in_filepath,
        out_filepath,
    )


def _output_ndjson(json_list: List[dict], out_filepath: pathlib.Path) -> None:
    with out_filepath.open("w") as fout:
        for obj in json_list:
            json.dump(obj, fout)
            fout.write("\n")


config = _get_config(YML_CONFIG)

if config["parser"] == "arcgis_features":
    """
    ArcGIS FeatureServers fetch as a json object containing a "features"
    attribute which contains a list of json objects.

    Parse files of this structure.
    """
    json_filepaths = INPUT_DIR.glob("*.json")
    for in_filepath in json_filepaths:
        with in_filepath.open() as fin:
            arcgis_feature_json = json.load(fin)

        out_filepath = _get_out_filepath(in_filepath, OUTPUT_DIR)
        _log_activity(config["state"], config["site"], in_filepath, out_filepath)

        _output_ndjson(arcgis_feature_json["features"], out_filepath)

elif config["parser"] == "json_list":
    """
    Parse files containing lists of json objects.
    """
    json_filepaths = INPUT_DIR.glob("*.json")
    for in_filepath in json_filepaths:
        with in_filepath.open() as fin:
            json_list = json.load(fin)

        for path_element in config.get("path", []):
            json_list = json_list[path_element]

        out_filepath = _get_out_filepath(in_filepath, OUTPUT_DIR)
        _log_activity(config["state"], config["site"], in_filepath, out_filepath)

        _output_ndjson(json_list, out_filepath)

else:
    logger.error("Parser '%s' was not recognized.", config["parser"])
    raise NotImplementedError(f"No shared parser available for '{config['parser']}'.")
