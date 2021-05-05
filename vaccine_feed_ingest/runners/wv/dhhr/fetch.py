#!/usr/bin/env python3

import os
import re
import sys

import requests
from bs4 import BeautifulSoup

from vaccine_feed_ingest.utils.log import getLogger

# dhhr.wv.gov has too small of a DH KEY for seclevel 2. Drop to seclevel 1.
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"

logger = getLogger(__file__)

index_url = "https://dhhr.wv.gov/News/2021/Pages/default.aspx"

output_dir = sys.argv[1]
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

index_page = requests.get(index_url).text

# The page is divided into monthly sections;
# within each section items are presented in reverse chronological order
# (most recent first).
#
# We want the first relevant link in the last section.
target_url = ""
section_selector = ".panel-default"
link_pattern = r"COVID-19-Vaccination-Clinics-\w+-\d+-\d+,-202\d\.aspx"

soup = BeautifulSoup(index_page, "html.parser")
section = soup.select(section_selector).pop()
for link in section.find_all_next("a"):
    href = link.get("href")
    if href and re.search(link_pattern, href):
        target_url = href
        break

if target_url == "":
    logger.error(f"Found no URL matching {link_pattern}")
    sys.exit(2)

file = open(os.path.join(output_dir, "output.html"), "w")
file.write(requests.get(target_url).text)
file.close()
