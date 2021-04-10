#!/usr/bin/env bash

set -Eeuo pipefail

data_dir=""
ndjson_file=""

if [ -n "${1}" ]; then
    data_dir="${1}"
else
    echo "Must pass an data_dir as first argument"
fi

if [ -n "${2}" ]; then
    ndjson_file="${2}"
else
    echo "Must pass an nsjson_file as second argument"
fi

echo "Parsing ${data_dir}/ into ${ndjson_file}"
