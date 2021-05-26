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
    main_data = tableau_entry
    name = main_data["Name-value"]
    address = main_data["Address-value"]

    contact = {}
    if main_data["Web Site-alias"] != "%null%":
        contact["website"] = main_data["Web Site-alias"]
    if main_data["Phone-value"] != "%null%":
        contact["phone"] = main_data["Phone-value"]

    return {
        # Name isn't sufficient, multiple "Acme Pharmacy" etc.
        "id": location_id_from_name(name + address),
        "contact": contact,
        "name": name,
        "address": address,
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
    indices_info = utils.getIndicesInfo(presModelMap, "List")
    data_dict = utils.getData(full_data, indices_info)
    # Transpose columns to rows (tableau-scraping uses pandas, but we don't strictly need to do that)
    # i.e. {'a': [a1, a2, a3], 'b': [b1, b2, b3]} --> [{'a': a1, 'b': b1}, {'a': a2, 'b': b2}, {'a': a3, 'b': b3}]
    transposed_data = map(
        dict,
        itertools.starmap(
            zip, zip(itertools.repeat(data_dict.keys()), zip(*data_dict.values()))
        ),
    )
    return (tableau_item_to_parsed_site(entry) for entry in transposed_data)


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
