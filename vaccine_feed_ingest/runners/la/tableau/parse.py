#!/usr/bin/env python

import json
import pathlib
import re
import sys

JSON_RE = re.compile(r"^\d+;\{.*\}\d+;(\{.*\})$")
SITE_RE = re.compile(r"^(\*\* )?(.*)\n(.*)\n(.*) LA $")

input_dir = pathlib.Path(sys.argv[2])
output_dir = pathlib.Path(sys.argv[1])

input_file = input_dir / "tableau.json"
output_file = output_dir / "data.parsed.ndjson"


with input_file.open() as fin:
    # Yes, this is ridiculous and fragile.  Why do you ask?
    m = JSON_RE.match(fin.read())
    if m is not None:  # We should bail out loudly if it is.
        data = json.loads(m.group(1))

        datavalues = data["secondaryInfo"]["presModelMap"]["dataDictionary"][
            "presModelHolder"
        ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][0][
            "dataValues"
        ]
        # datavalues looks like it's a string-intern table, organized as follows:
        #
        # 0..N locations
        #
        # "Map" (maybe string interning?)
        # "%null%"
        # " "
        # "Website" (ditto)
        # "Yes"
        # "NEW"
        #
        # 0..M counties
        #
        # 0..P phone numbers
        #
        # 0..Q websites
        #
        # Somewhere in data["secondaryInfo"]["presModelMap"]["vizData"], there are
        # offsets into datavalues that get reconstructed into the table, but
        # details are still sketchy.

        sites = []
        for v in datavalues:
            if v == "Map":  # We've hit the end of the locations.
                break
            m = SITE_RE.match(v)
            if not m:  # Failed to match!  We should log this.
                continue
            site = {
                "providerName": m.group(2),
                "streetAddress": m.group(3),
                "city": m.group(4),
            }
            if m.group(1) == "** ":
                site["minimumAge"] = "16"
            sites.append(site)

        with open(output_file, "w") as fout:
            for site in sites:
                json.dump(site, fout)
                fout.write("\n")
