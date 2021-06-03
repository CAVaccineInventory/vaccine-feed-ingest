#!/usr/bin/env python3

import json
import os
import pathlib
import re
import sys
from typing import List

import yaml
from bs4 import BeautifulSoup

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
            logger.error(exc)

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


def _prepmod_find_data_item(parent, label, offset):
    row_matches = [x for x in parent.find_all(["p", "div"]) if label in x.get_text()]
    try:
        content = row_matches[-1].contents[offset]
    except IndexError:
        return ""
    return content.strip() if isinstance(content, str) else content.get_text().strip()


config = _get_config(YML_CONFIG)
EXTRACT_CLINIC_ID = re.compile(r".*clinic(\d*)\.png")

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

elif config["parser"] == "prepmod":
    """
    Parse HTML 'prepmod' data.
    """
    input_filenames = INPUT_DIR.glob("*.html")

    for filename in input_filenames:
        out_filepath = _get_out_filepath(filename, OUTPUT_DIR)
        text = open(filename, "r").read()
        soup = BeautifulSoup(text, "html.parser")

        # classes only used on titles for search results
        with open(out_filepath, "w") as fout:
            for title in soup.select(".text-xl.font-black"):
                parent = title.parent
                combined_name = title.get_text().strip()
                name, date = combined_name.rsplit(" on ", 1)
                address = title.find_next_sibling("p").get_text().strip()
                vaccines = _prepmod_find_data_item(parent, "Vaccinations offered", -2)
                ages = _prepmod_find_data_item(parent, "Age groups served", -1)
                additional_info = _prepmod_find_data_item(
                    parent, "Additional Information", -1
                )
                hours = _prepmod_find_data_item(parent, "Clinic Hours", -1)
                available_count = (
                    _prepmod_find_data_item(parent, "Available Appointments", -1) or 0
                )
                special = _prepmod_find_data_item(parent, "Special Instructions", -1)
                find_clinic_id = EXTRACT_CLINIC_ID.match(
                    parent.find_next_sibling("div", "map-image").find("img")["src"]
                )
                clinic_id = find_clinic_id.group(1)
                data = {
                    "name": name,
                    "date": date,
                    "address": address,
                    "vaccines": vaccines,
                    "ages": ages,
                    "info": additional_info,
                    "hours": hours,
                    "available": available_count,
                    "special": special,
                    "clinic_id": clinic_id,
                }
                json.dump(data, fout)
                fout.write("\n")
else:
    logger.error("Parser '%s' was not recognized.", config["parser"])
    raise NotImplementedError(f"No shared parser available for '{config['parser']}'.")
