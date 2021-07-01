#!/usr/bin/env bash
# This scraper is inactive as the data source is no longer being updated.

set -Eeuo pipefail

output_dir=""
if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

(cd "$output_dir" && curl --silent "https://ilvaccine-api.us-east-1.linodeobjects.com/vts.ndjson" -o 'ilvaccine_org.ndjson')


# This scraper is inactive as the data source is no longer being updated.
