#!/usr/bin/env python3

import json
import os
import re
import sys

import requests
from bs4 import BeautifulSoup

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

jcdh_org_url = "https://www.jcdh.org/"

output_dir = sys.argv[1]
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

logger.info("Reading page from %s", jcdh_org_url)
with requests.Session() as session:
    response = session.get(jcdh_org_url)
    logger.info("Response: %s", response)
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
logger.info("Downloading PDF from %s", pdf_url)
with requests.Session() as session:
    pdf_response = session.get(pdf_url)
    logger.info("Response: %s", pdf_response)

# Write the PDF.
output_path = os.path.join(output_dir, "output.pdf")
with open(output_path, "wb") as f:
    f.write(pdf_response.content)
logger.info("PDF written to %s", output_path)

# Write an NDJSON file with fetch metadata, like the URL,
# to provide this information to the rest of the data pipeline.
metadata_output_path = os.path.join(output_dir, "metadata.ndjson")
with open(metadata_output_path, "w") as f:
    json.dump(
        {
            "fetched_from_uri": pdf_url,
        },
        f,
    )
    f.write("\n")
logger.info("Fetch metadata written to %s", metadata_output_path)
