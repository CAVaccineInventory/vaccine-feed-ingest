#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

SESSION_ID="$(curl --silent -I 'https://public.tableau.com/views/VaccineAdministrationLocations/VaccineAdministrationLocations?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Fpublic.tableau.com%2F&:embed_code_version=3&:tabs=no&:toolbar=no&:showAppBanner=false&iframeSizedToWindow=true&:loadOrderID=0' | grep -i '^X-Session-Id' | awk '{print $2}' | tr -d '\r')"

curl "https://public.tableau.com/vizql/w/VaccineAdministrationLocations/v/VaccineAdministrationLocations/bootstrapSession/sessions/${SESSION_ID}" \
    -d 'renderMapsClientSide=true&isBrowserRendering=true&browserRenderingThreshold=9999999' \
    -H 'content-type: application/x-www-form-urlencoded' \
    -H 'accept: application/json' \
    -d 'sheet_id=VaccineAdministrationLocations' \
    -o "${output_dir}/tableau.json"
