# vaccine-feed-ingest

Pipeline for ingesting nationwide feed of vaccine facilities

## Usage

### Quick Setup For MacOS Homebrew users

```sh
brew install python3
brew install poetry
poetry install
```

### Setup Environment Once

1. Install required system deps (Ubuntu/Debian):

    ```sh
    sudo apt-get install libbz2-dev liblzma-dev libreadline-dev libsqlite3-dev
    ```

1. Install `pyenv`:

    ```sh
    curl https://pyenv.run | bash
    ```

1. Add to `.bashrc`:

    ```sh
    export PATH="/home/codespace/.pyenv/bin:$PATH"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
    ```

1. Install project python version:

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

1. Install app dependancies:

    ```sh
    poetry install
    ```

1. (optional) For development:

    ```sh
    poetry install --extras lint
    ```


### Run Pipelines

- List all available sites:

    ```sh
    poetry run vaccine-feed-ingest/run.py available-sites
    ```

- Run fetch for all sites in CA:

    ```sh
    poetry run vaccine-feed-ingest/run.py fetch --state=ca --output-dir=out
    ```

- Run all stages for all sites:

    ```sh
    poetry run vaccine-feed-ingest/run.py all-stages --output-dir=out
    ```

- Run fetch for just one site:

    ```sh
    # Writes ingest data to out/ca/sf_gov/ directory
    poetry run vaccine-feed-ingest/run.py fetch ca/sf_gov --output-dir=out
    ```

- Run parse for just one site:

    ```sh
    # Parses data in out/ca/sf_gov/ directory into out/ca/sf_gov/locations.ndjson
    poetry run vaccine-feed-ingest/run.py parse ca/sf_gov --output-dir=out
    ```

- Run normalize for just one site:

    ```sh
    # Parses data in out/ca/sf_gov/ directory into out/ca/sf_gov/locations.ndjson
    poetry run vaccine-feed-ingest/run.py normalize ca/sf_gov --output-dir=out
    ```
