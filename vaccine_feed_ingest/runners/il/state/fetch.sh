#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""
if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

(cd "$output_dir" && curl --silent "https://coronavirus.illinois.gov/content/dam/soi/en/web/coronavirus/documents/vaccination-locations.csv" -o 'il_state.csv')
