#!/usr/bin/env python3

import json
import sys

from bs4 import BeautifulSoup

input_dir = sys.argv[2]
output_dir = sys.argv[1]

input_file = input_dir + "/fl-vaccine-locator.html"
output_file = output_dir + "/data.parsed.ndjson"


with open(input_file) as fin:
    soup = BeautifulSoup(fin.read(), "html.parser")
    script_tag = soup.select_one("#vaccine-sites-english .sub-copy--wrapper script")
    script_content = script_tag.string

    # Script looks like this:
    #
    # jQuery(document).ready(function($) {var map2 = $(\"#map2\").maps({
    #  GINORMOUS amount of keys, one of which is "places" which we need
    # }).data("wpgmp_maps");});
    #

    js_params = script_content.replace(
        'jQuery(document).ready(function($) {var map2 = $("#map2").maps(', ""
    ).replace(').data("wpgmp_maps");});', "")

    parsed = json.loads(js_params)
    places = parsed["places"]

    with open(output_file, "w") as fout:
        for place in places:
            json.dump(place, fout)
            fout.write("\n")
