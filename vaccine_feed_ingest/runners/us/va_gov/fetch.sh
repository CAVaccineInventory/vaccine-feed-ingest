#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

# Per VA_gov spec available at https://developer.va.gov/explore/facilities/docs/facilities?version=current
(cd "$output_dir" && curl -X GET 'https://sandbox-api.va.gov/services/va_facilities/v0/facilities/all' \
--header "apikey: ${TOKEN_FETCH_US_VA}" --create-dirs -o 'va_gov.json')



##takes an API key that you can self-service create from https://developer.va.gov/apply -- or I can provide
##this secret out-of-band via Discord or whatever

##Note: Sandbox appears to be appropriate environment for read-only access; it appears prod is only for authorized developers that are building things that 
##can change state on the VA server side.  Data is accurate as far as I can tell against what's available in real time on VA website.
