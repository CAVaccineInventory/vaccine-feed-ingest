#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""
input_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

if [ -n "${2}" ]; then
    input_dir="${2}"
else
    echo "Must pass an input_dir as second argument"
fi

### Replace the following with your implementation ###

echo "Parsing ${input_dir} into ${output_dir}"
cp "${input_dir}/ilvaccine_org.ndjson" "${output_dir}/ilvaccine_org.parsed.ndjson"
