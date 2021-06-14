#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

curl "https://ldh.la.gov/index.cfm?md=directory&tmp=_snip_vaccination_locations&parish=&city=&zip=&vaccinationDate=&vaccineBrand=&eventSetup=&nowrap=1" \
    -o "${output_dir}/la_dph.html"
