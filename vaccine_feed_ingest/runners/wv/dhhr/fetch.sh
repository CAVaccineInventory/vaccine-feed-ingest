#!/usr/bin/env bash

set -Eeu -o pipefail

output_dir=""
if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo >&2 "Must pass an output_dir as first argument"
    exit 1
fi

cd "$output_dir"
curl --no-progress-meter 'https://dhhr.wv.gov/News/2021/Pages/COVID-19-Vaccination-Clinics-March-2-7,-2021.aspx' -o 'dhhr.html'
