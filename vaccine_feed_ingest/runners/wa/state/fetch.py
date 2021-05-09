#!/usr/bin/env python3
import os
import sys

import requests

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


# See https://apim-vaccs-prod.azure-api.net/web/graphql for the full interface w/documention
# editorconfig-checker-disable
QUERY = """
{
  searchLocations(
    searchInput: {
      # Area of Washinton State
      radiusMiles: 71362
      # A very large paging size
      paging: { pageSize: 1000000, pageNum: 1 }
    }
  ) {
    paging {
      total
      pageNum
      pageSize
    }
    locations {
      locationId
      locationName
      locationType
      providerId
      providerName
      departmentId
      departmentName
      addressLine1
      addressLine2
      city
      state
      county
      zipcode
      latitude
      longitude
      description
      contactFirstName
      contactLastName
      fax
      phone
      email
      schedulingLink
      vaccineAvailability
      infoLink
      timeZoneId
      directions
      updatedAt
      rawDataSourceName
      vaccineTypes
      accessibleParking
      additionalSupports
      commCardAvailable
      commCardBrailleAvailable
      driveupSite
      interpretersAvailable
      interpretersDesc
      supportUrl
      waitingArea
      walkupSite
      wheelchairAccessible
      scheduleOnline
      scheduleByPhone
      scheduleByEmail
      walkIn
      waitList
    }
    summary {
      vaccineAvailability
      total
    }
  }
}
"""
# editorconfig-checker-enable

GRAPHQL_ENDPOINT = "https://apim-vaccs-prod.azure-api.net/web/graphql"

output_dir = sys.argv[1]
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

r = requests.post(GRAPHQL_ENDPOINT, json={"query": QUERY})
file = open(os.path.join(output_dir, "output.json"), "w")
file.write(r.text)
file.close()
