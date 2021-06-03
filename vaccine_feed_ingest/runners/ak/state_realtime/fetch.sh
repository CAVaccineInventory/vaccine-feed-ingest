#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

# anchoragecovidvaccine.org uses CSV updated with appointment
# information every minute.  It does not include everything from
# ak/clinic_list.
(cd "$output_dir" && curl --silent "https://anchoragecovidvaccine.org/data/complete_prepmod_format.csv" -o 'ak_realtime.csv')
