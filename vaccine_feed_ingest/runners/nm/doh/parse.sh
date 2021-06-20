#!/usr/bin/env bash

set -Eeuo pipefail

if [[ -z ${1+x} ]]; then
    echo 'Must pass an output_dir as first argument'
    exit 64
fi
output_dir="${1}"

if [[ -z ${2+x} ]]; then
    echo 'Must pass an input_dir as second argument'
    exit 64
fi
input_dir="${2}"

for f in $(find "${input_dir}" -type f -name '*.json'); do
    output_file="${output_dir}/$(basename ${f} .json).ndjson"
    jq --compact-output '.data | .[]' <"${f}" >"${output_file}"
done
