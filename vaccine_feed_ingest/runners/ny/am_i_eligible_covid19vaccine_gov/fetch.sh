#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

curl --silent https://am-i-eligible.covid19vaccine.health.ny.gov/api/list-providers -o "${output_dir}/list-providers.json"
