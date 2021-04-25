#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

curl --silent "https://dph.georgia.gov/locations/covid-vaccination-site" -o "${output_dir}/locations.html"
