---
name: runner/fetch
about: Fetch a new site
title: fetch SITE from STATE
labels: runner/fetch
assignees: ''

---

[![learn our pipeline: fetch](https://img.shields.io/static/v1?label=learn%20our%20pipeline&message=fetch&style=social)](https://github.com/CAVaccineInventory/vaccine-feed-ingest/wiki/Runner-pipeline-stages#fetch)

Fetch data from: <!-- ENTER URL HERE -->

Put your script in a file named: <!-- ca/sf_gov/fetch.sh -->

Store it (without processing) in a new file created in the directory passed as the first argument (`sys.argv[1]`).

Check the wiki to learn more about the purpose of the fetch stage and how to get set up for development!

### Tips:

1. While working on your code, run it at any point:
    ```sh
    poetry run vaccine-feed-ingest fetch <state>/<site>
    ```
