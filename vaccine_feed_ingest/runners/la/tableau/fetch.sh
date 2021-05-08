#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

SESSION_ID="$(curl --silent -I 'https://public.tableau.com/views/pharmacies_desktop/Pharmacies_desktop?%3Aembed=y&%3AshowVizHome=no&%3Adisplay_count=y&%3Adisplay_static_image=y&%3AbootstrapWhenNotified=true&%3Alanguage=en&:embed=y&:showVizHome=n&:apiID=host0#navType=1&navSrc=Parse' | grep -i '^X-Session-Id' | awk '{print $2}' | tr -d '\r')"

curl "https://public.tableau.com/vizql/w/pharmacies_desktop/v/Pharmacies_desktop/bootstrapSession/sessions/${SESSION_ID}" \
    -d 'renderMapsClientSide=true&isBrowserRendering=true&browserRenderingThreshold=9999999' \
    -H 'content-type: application/x-www-form-urlencoded' \
    -H 'accept: application/json' \
    -d 'sheet_id=Pharmacies_desktop' \
    -o "${output_dir}/tableau.json"
