#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

echo "Fetching into ${output_dir}"
cd "${output_dir}"

# Build ID changes frequently.
curl --silent -o index.html "https://covidvaccination.dph.illinois.gov/"
BUILD_ID=$(sed -Ene 's/.*"buildId":"([^"]+)".*/\1/p' index.html)
echo "Build ID is ${BUILD_ID}"

curl --silent -o events.json "https://covidvaccination.dph.illinois.gov/_next/data/${BUILD_ID}/events.json"
