#!/bin/zsh

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

# Per VA_gov spec
(cd "$output_dir" && curl -X GET 'https://sandbox-api.va.gov/services/va_facilities/v0/facilities/all' \
--header "apikey: ${TOKEN_FETCH_US_VA}" --create-dirs -o 'va_gov.json')
