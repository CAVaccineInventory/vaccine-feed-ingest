#!/usr/bin/env python

import glob
import json
import sys

site_dir = sys.argv[1]
out_ndjson = sys.argv[2]

files = glob.glob(f"{site_dir}/*.json")

with open(out_ndjson, "w") as fout:
    for file in files:
        with open(file) as fh:
            obj = json.load(fh)

        for site in obj["data"]["sites"]:
            json.dump(site, fout)
            fout.write("\n")
