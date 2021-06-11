#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

curl "https://www.cahabamedicalcare.com/covid19" \
    -o "${output_dir}/al_cahaba.html"
