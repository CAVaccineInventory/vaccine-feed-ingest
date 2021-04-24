#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""
if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

(cd "$output_dir" && curl --silent "https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_Vaccine_Provider_Sites/MapServer/0/query?where=1%3D1&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&outFields=*&returnGeometry=true&returnTrueCurves=false&returnIdsOnly=false&returnCountOnly=false&returnZ=false&returnM=false&returnDistinctValues=false&returnExtentsOnly=false&f=geojson" -o 'wi_arcgis_map.geojson')