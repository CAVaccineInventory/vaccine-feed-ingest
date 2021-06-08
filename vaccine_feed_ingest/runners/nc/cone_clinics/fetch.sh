#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

curl "https://calendar.google.com/calendar/ical/fj6fi61ophj8p0mlvck08r3gg4%40group.calendar.google.com/public/basic.ics" \
    -o "${output_dir}/cone_clinics.ics"
