#!/usr/bin/env bash

set -Eeuo pipefail

ndjson_file=""
normalized_file=""

if [ -n "${1}" ]; then
    ndjson_file="${1}"
else
    echo "Must pass an ndjson_file as first argument"
fi

if [ -n "${2}" ]; then
    normalized_file="${2}"
else
    echo "Must pass an normalized_file as second argument"
fi

echo "Normalizing ${ndjson_file} into ${normalized_file}"
