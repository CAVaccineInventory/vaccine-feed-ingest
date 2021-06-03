#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

# https://www.healthy.arkansas.gov/programs-services/topics/covid-19-map-of-1-a-pharmacy-locations has an IFRAMEd Datawrapper.
(cd "$output_dir" && curl --silent "https://datawrapper.dwcdn.net/bY8T3/45/" -o 'data.html')
