#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""
if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

(cd "$output_dir" && curl --silent "https://dhhr.wv.gov/News/2021/Pages/COVID-19-Vaccination-Clinics-March-2-7,-2021.aspx" -o 'dhhr.html')
