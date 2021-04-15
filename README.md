# vaccine-feed-ingest

Pipeline for ingesting nationwide feed of vaccine facilities

## Usage

### Setup Developer Environment for Mac

1. Install `homebrew` if you don't have it:

    ```sh
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```

1. Install `python` version `3.9` or higher:

    ```sh
    brew install python@3.9
    ```

1. *(optional)* If you need multiple python versions then use `pyenv`:

    ```sh
    brew install pyenv
    pyenv install
    ```

1. Install `poetry`

    ```sh
    brew install poetry
    ```

1. Install app dependancies with extras for development:

    ```sh
    poetry install --extras lint
    ```

### Setup Developer Environment for Ubuntu/Debian

1. Install required system deps:

    ```sh
    sudo apt-get install libbz2-dev liblzma-dev libreadline-dev libsqlite3-dev
    ```

1. Install `python` version `3.9` or higher:

    ```sh
    sudo-apt install python3.9
    ```

1. *(optional)* If you need multiple python versions then use `pyenv`:

    ```sh
    curl https://pyenv.run | bash
    ```

1. *(optional)* Add `pyenv` to `.bashrc`:

    ```sh
    export PATH="$HOME/.pyenv/bin:$PATH"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
    ```

1. *(optional)* Install project python version:

    ```sh
    pyenv install
    ```

1. Install `poetry`:

    ```sh
    curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
    ```

1. Add to `.bashrc`:

    ```sh
    export PATH="$HOME/.poetry/bin:$PATH"
    ```

1. Install app dependancies with extras for development:

    ```sh
    poetry install --extras lint
    ```

### Run Pipelines

Run pipelines using the `vaccine-feed-ingest` command in the poetry `venv`:

You can enter the poetry `venv` with the `poetry shell` command, or prefix each command
with `poetry run`.

```sh
poetry run vaccine-feed-ingest <fetch|parse|normalize> <site>
```

### Example Commands

- List all available sites:

    ```sh
    poetry run vaccine-feed-ingest available-sites
    ```

- Run fetch for just one site:

    ```sh
    poetry run vaccine-feed-ingest fetch ca/sf_gov
    ```

- Run fetch for all sites in CA:

    ```sh
    poetry run vaccine-feed-ingest fetch --state=ca
    ```

- Run all stages for all sites:

    ```sh
    poetry run vaccine-feed-ingest all-stages
    ```

- Run all stages for two sites:

    ```sh
    poetry run vaccine-feed-ingest all-stages ca/sf_gov us/vaccinespotter_org
    ```

## Contributing

### How to

1. Configure your python environment as specified above.
2. Choose an unassigned website from [National Websites to Scrape](https://airtable.com/shr55fpTXObYmdk48) ([Commenter access](https://airtable.com/invite/l?inviteId=invRAMMkTCYH5FAoh&inviteToken=651c8220466fc266cd936182bf3aea6643606a44f3f1414784e4d0964e2a163a))
3. Submit a PR to his repo that scrapes data from that source

### Runner

There are 3 stages to writing a scraper, and you can write as many of the stages as you want. Even writing just the first stage is a big help.

Each scraper is stored in [vaccine_feed_ingest/runners/
](https://github.com/CAVaccineInventory/vaccine-feed-ingest/tree/main/vaccine_feed_ingest/runners). Runners are grouped by state, and named the same as the website with `_` replacing `.` e.g. `vaccine_feed_ingest/runners/ca/sf_gov`.

#### Stages

1. **Fetch**: Retrieve the raw data from external source and store it unchanged
2. **Parse**: Convert the raw data into json records and store it as ndjson
3. **Normalize**: Transform the parsed json records into VaccinateCA schema

Each stage is an executeable script (with a `+x` bit) named after the stage e.g. `fetch.sh` or `fetch.py`. The script is passed an output directory as the first argument, and an input directory as the second argument.

Every file written to the output directory that doesn't start with `.` or `_` is stored and passed along to the next stage.

#### Expected Output

1. **Fetch**: `.geojson`, `.html`, `.zip`, etc.
2. **Parse**: `*.parsed.ndjson`
3. **Normalize**:  `*.normalized.ndjson`

### Development

You can iterate on one stage at a time by running just that stage for a single site. Output for runs are stored by default in a `out` directory at the root of the repo.

If you are iterating on parsing, then you only need to run `fetch` stage once, and then run `parse` as many times as you need.

Example:

```sh
poetry run vaccine-feed-ingest fetch ca/sf_gov
```


### Production

In production, all stages for all runners are run, and outputs are stored to the `vaccine-feeds` bucket on GCS. If you need to test the full pipeline with GCS, use the `vaccine-feeds-dev` bucket (you will need to be granted access).

Results are also periodically committed to [`vaccine-feed-ingest-results`](https://github.com/CAVaccineInventory/vaccine-feed-ingest-results).
