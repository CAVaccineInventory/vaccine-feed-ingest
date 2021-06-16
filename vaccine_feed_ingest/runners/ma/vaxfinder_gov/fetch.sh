#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

cd "$output_dir"

curl --silent "https://www.mass.gov/doc/covid-19-vaccine-locations-for-currently-eligible-recipients-csv/download" -o 'ma.csv'
