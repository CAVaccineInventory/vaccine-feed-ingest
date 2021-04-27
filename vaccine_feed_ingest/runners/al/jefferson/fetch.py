#!/usr/bin/env python3

import os
import re
import sys

import requests
from bs4 import BeautifulSoup

jcdh_org_url = "https://www.jcdh.org/"

output_dir = sys.argv[1]
if output_dir is None:
    print("Must pass an output_dir as first argument")

session = requests.Session()
response = session.get(jcdh_org_url)
soup = BeautifulSoup(response.text, "html.parser")

spans = soup.find_all("span")

for span in spans:
    if span.find(text=re.compile("List of COVID-19 Vaccine Sites in Jefferson County")):
        vax_sites_span = span
        break

# <a href="https://www.jcdh.org/SitePages/Misc/PdfViewer?AdminUploadId=1368"
# target="_blank" title="https://www.jcdh.org/SitePages/Misc/PdfViewer?AdminUploadId=1368 ctrl + click to follow link">
# <u>here</u></a>
pdf_url = vax_sites_span.select_one("a").get("href")
pdf_response = session.get(pdf_url)

pdf_response = requests.get(pdf_url)
output_path = os.path.join(output_dir, "output.pdf")
with open(output_path, "wb") as f:
    f.write(pdf_response.content)
