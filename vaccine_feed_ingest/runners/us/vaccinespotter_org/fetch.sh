#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

echo "Fetching into ${output_dir}"
curl https://www.vaccinespotter.org/api/v0/US.json >"${output_dir}/data.geojson"
