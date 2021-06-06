#!/usr/bin/env bash

set -Eeuo pipefail

output_dir=""

if [ -n "${1}" ]; then
    output_dir="${1}"
else
    echo "Must pass an output_dir as first argument"
fi

curl 'https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true' \
    -X POST \
    -H 'Host: wabi-us-gov-iowa-api.analysis.usgovcloudapi.net' \
    -H 'Content-Type: application/json' \
    -o "${output_dir}/powerbi.json" \
    --compressed \
    --data-raw '{
    "version": "1.0.0",
    "queries": [
        {
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "v",
                                        "Entity": "VaccineLocator (3)",
                                        "Type": 0
                                    }
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "v"
                                                }
                                            },
                                            "Property": "Provider_Address"
                                        },
                                        "Name": "VaccineLocator (3).Provider_Address"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "v"
                                                }
                                            },
                                            "Property": "INVENTORY"
                                        },
                                        "Name": "VaccineLocator (3).INVENTORY"
                                    },
                                    {
                                        "Aggregation": {
                                            "Expression": {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "v"
                                                        }
                                                    },
                                                    "Property": "PROVIDER_NAME"
                                                }
                                            },
                                            "Function": 3
                                        },
                                        "Name": "Min(VaccineLocator (3).PROVIDER_NAME)"
                                    },
                                    {
                                        "Aggregation": {
                                            "Expression": {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "v"
                                                        }
                                                    },
                                                    "Property": "INSTRUCTIONS"
                                                }
                                            },
                                            "Function": 3
                                        },
                                        "Name": "Min(VaccineLocator (3).INSTRUCTIONS)"
                                    }
                                ]
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {
                                            "Projections": [
                                                0,
                                                2,
                                                3
                                            ]
                                        }
                                    ]
                                },
                                "Secondary": {
                                    "Groupings": [
                                        {
                                            "Projections": [
                                                1
                                            ]
                                        }
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 4,
                                    "Primary": {
                                        "Top": {}
                                    },
                                    "Secondary": {
                                        "Top": {}
                                    }
                                },
                                "SuppressedJoinPredicates": [
                                    2,
                                    3
                                ],
                                "Version": 1
                            }
                        }
                    }
                ]
            },
            "CacheKey": "{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"v\",\"Entity\":\"VaccineLocator (3)\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"v\"}},\"Property\":\"Provider_Address\"},\"Name\":\"VaccineLocator (3).Provider_Address\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"v\"}},\"Property\":\"INVENTORY\"},\"Name\":\"VaccineLocator (3).INVENTORY\"},{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"v\"}},\"Property\":\"PROVIDER_NAME\"}},\"Function\":3},\"Name\":\"Min(VaccineLocator (3).PROVIDER_NAME)\"},{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"v\"}},\"Property\":\"INSTRUCTIONS\"}},\"Function\":3},\"Name\":\"Min(VaccineLocator (3).INSTRUCTIONS)\"}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0,2,3]}]},\"Secondary\":{\"Groupings\":[{\"Projections\":[1]}]},\"DataReduction\":{\"DataVolume\":4,\"Primary\":{\"Top\":{}},\"Secondary\":{\"Top\":{}}},\"SuppressedJoinPredicates\":[2,3],\"Version\":1}}}]}",
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "fa784c68-3c2d-4bf6-9247-dcab1d6b0bc1",
                "Sources": [
                    {
                        "ReportId": "416f9c4b-01ad-4331-b95e-5841ba2294ba"
                    }
                ]
            }
        }
    ],
    "cancelQueries": [],
    "modelId": 319611
}'
