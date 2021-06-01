#!/usr/bin/env python

import itertools
import json
import pathlib
import re
import sys

from tableauscraper import utils

from vaccine_feed_ingest.utils.parse import location_id_from_name


def tableau_item_to_parsed_site(tableau_entry):
    """Put the tableau entry in something closer to the normalized format."""
    main_data, extra_data = tableau_entry
    name, street, city_state = main_data["Site-value"].split("\n")
    city_state = city_state.strip()
    if city_state.endswith(" LA"):
        city = city_state[:-3]
    else:
        city = city_state
    state = "LA"
    address = {"street1": street, "city": city, "state": state}

    if name.startswith("** "):
        name = name[3:]
        minimum_age = 16
    else:
        minimum_age = 18

    id = location_id_from_name(name)

    contact = {}
    if main_data["Dimension-value"] == "Website":
        contact["website"] = main_data["Value-alias"]
    elif extra_data["Dimension-value"] == "Website":
        contact["website"] = extra_data["Value-alias"]

    if main_data["Phone-value"] != "%null%":
        contact["phone"] = main_data["Phone-value"]

    return {
        "id": id,
        "contact": contact,
        "name": name,
        "address": address,
        "minimum_age": minimum_age,
    }


def parse_tableau(file_contents):
    """
    This is a weird blob containing *two* JSON encoded dictionaries.
    Each is preceded by its length in bytes, but using a regex (instead of counting) is simple enough.
    Follows the approach in tableau-scraping. See the links below:
    https://github.com/bertrandmartel/tableau-scraping/blob/9dba25af057ac29f921a75df374943060ab79b0a/tableauscraper/TableauScraper.py#L77-L84
    https://github.com/bertrandmartel/tableau-scraping/blob/9dba25af057ac29f921a75df374943060ab79b0a/tableauscraper/dashboard.py#L35
    """
    info_and_data = re.search(r"\d+;({.*})\d+;({.*})", file_contents, re.MULTILINE)
    data = json.loads(info_and_data.group(2))
    presModelMap = data["secondaryInfo"]["presModelMap"]
    dataSegments = presModelMap["dataDictionary"]["presModelHolder"][
        "genDataDictionaryPresModel"
    ]["dataSegments"]
    full_data = utils.getDataFull(presModelMap, dataSegments)
    indices_info = utils.getIndicesInfo(presModelMap, "Vaccination Sites")
    data_dict = utils.getData(full_data, indices_info)
    # Transpose columns to rows (tableau-scraping uses pandas, but we don't strictly need to do that)
    # i.e. {'a': [a1, a2, a3], 'b': [b1, b2, b3]} --> [{'a': a1, 'b': b1}, {'a': a2, 'b': b2}, {'a': a3, 'b': b3}]
    transposed_data = map(
        dict,
        itertools.starmap(
            zip, zip(itertools.repeat(data_dict.keys()), zip(*data_dict.values()))
        ),
    )
    # Data contains at least one bad value; filter it out. See https://github.com/CAVaccineInventory/vaccine-feed-ingest/issues/621
    filtered_transposed_data = (
        row for row in transposed_data if row["Site-value"] != "%null%"
    )
    # Adjacent rows are actually duplicates; some have map, some have website. Combine into one.
    doubled_filtered_transposed_data = zip(
        filtered_transposed_data, filtered_transposed_data
    )
    return (
        tableau_item_to_parsed_site(entry) for entry in doubled_filtered_transposed_data
    )


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.json")

for in_filepath in json_filepaths:
    with in_filepath.open() as fin:
        sites = parse_tableau(fin.read())

    filename = in_filepath.name.split(".", maxsplit=1)[0]
    out_filepath = output_dir / f"{filename}.parsed.ndjson"

    with out_filepath.open("w") as fout:
        for site in sites:
            json.dump(site, fout)
            fout.write("\n")
