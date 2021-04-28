#!/usr/bin/env python

import io
import pathlib
import re
import sys

import requests
from bs4 import BeautifulSoup

"""
    This script handles more than a standard fetcher (as discussed in the PR
    with multiple members of the repo staff,) in order to get the data from the
    embedded Google map in the colorado.gov vaccine page.

    The resulting data from the Google map is a string (since _pageData is
    actually a Javascript variable in the page,) so this script outputs that
    string to a text file which can be parsed in the parser step.
"""

output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "colorado_gov.txt"

url = (
    "https://www.google.com/maps/d/u/0/viewer?mid=1x9KT3SJub0igOTnhFtdRYmceZuBXMWvK&ll"
    "=38.98747216882165%2C-105.9556642978642&z=6"
)

r = requests.get(url)

soup = BeautifulSoup(r.text, "html.parser")

# get script div from html page
script = soup.div.script

buf = io.StringIO(str(script))
item = buf.readline()
while item:
    if re.match("  var _pageData", item):
        pagedata = item
    item = buf.readline()

# extract _pageData data from assignment string

# locate equal sign
equalsign = pagedata.find("=", 0, 20)

# calculate first quote to be removed
firstquote = equalsign + 3

# locate last quote to remove everything after the data
lastquote = pagedata.rfind('"')

raw_pagedata = pagedata[firstquote:lastquote]

with open(output_file, "w") as fout:
    fout.write(raw_pagedata)
    fout.write("\n")
