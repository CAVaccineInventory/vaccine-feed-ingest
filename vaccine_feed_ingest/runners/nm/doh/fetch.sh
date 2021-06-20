#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

today=$(date --iso-8601=date)
for d in $(seq 0 14); do
    date=$(date --iso-8601=date --date="${today} + ${d} days")
    curl \
        --silent \
        --show-error \
        --output "${output_dir}/nmdoh-${date}.json" \
        "https://cvvaccine.nmhealth.org/api/GetPublicCalendar?county=&after=${date}&before=${date}"
done
