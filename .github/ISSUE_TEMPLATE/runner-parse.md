---
name: runner/parse
about: Parse the output of a fetch.
title: parse SITE from STATE
labels: runner/parse
assignees: ''

---

[![learn our pipeline: parse](https://img.shields.io/static/v1?label=learn%20our%20pipeline&message=parse&style=social)](https://github.com/CAVaccineInventory/vaccine-feed-ingest/wiki/Runner-pipeline-stages#parse)

Parse existing data into [`.ndjson`](http://ndjson.org/).

Read all files in the directory passed as the second argument (`sys.argv[2]`), convert them to `.ndjson`, and then output them to new files in the directory passed as the first argument (`sys.argv[1]`).

Check the wiki to learn more about the purpose of the parse stage and how to get set up for development!

### Tips

1. Fetch data for this site before you start developing:
    ```sh
    poetry run vaccine-feed-ingest fetch <state>/<site>
    ```

1. While working on your code, run it at any point:
    ```sh
    poetry run vaccine-feed-ingest parse <state>/<site>
    ```

### Example
[Fetched files for `md/arcgis`](https://github.com/CAVaccineInventory/vaccine-feed-ingest-results/tree/main/md/arcgis/raw) are converted to [parsed files](https://github.com/CAVaccineInventory/vaccine-feed-ingest-results/tree/main/md/arcgis/parsed)
