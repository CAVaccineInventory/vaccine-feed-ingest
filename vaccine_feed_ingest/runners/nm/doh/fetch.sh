#!/usr/bin/env bash

set -Eeuo pipefail

if [[ -z ${1+x} ]]; then
    echo 'Must pass an output_dir as first argument'
    exit 64
fi
output_dir="${1}"

today=$(date --iso-8601=date)
for d in $(seq 0 14); do
    # We could just use a relative date here, but that could give surprising
    # results if the script started just before midnight and ran into the
    # following day.
    #
    # We circumvent this by using "today", set before fetching begins, as the
    # reference date.
    date=$(date --iso-8601=date --date="${today} + ${d} days")
    curl \
        --silent \
        --show-error \
        --output "${output_dir}/nmdoh-${date}.json" \
        "https://cvvaccine.nmhealth.org/api/GetPublicCalendar?county=&after=${date}&before=${date}"
done
