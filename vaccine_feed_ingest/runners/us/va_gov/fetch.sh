#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

# Per VA_gov spec
(cd "$output_dir" && curl -X GET "https://api.va.gov/v1/facilities/va?bbox%5B%5D=-180&bbox%5B%5D=-90&bbox%5B%5D=180&bbox%5B%5D=90&type=health&services%5B%5D=Covid19Vaccine&page=1&per_page=2000&radius=25000&latitude=37.408123149415275&longitude=-93.14343299172322" --create-dirs -o 'va_gov.json')
