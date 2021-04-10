# vaccine-feed-ingest

Pipeline for ingesting nationwide feed of vaccine facilities

## Usage

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

- Run fetch for just one site:

    ```sh
    poetry run vaccine-feed-ingest/run.py fetch ca/sf_gov
    ```

- Run fetch for all sites in CA:

    ```sh
    poetry run vaccine-feed-ingest/run.py fetch --state=ca
    ```

- Run all stages for all sites:

    ```sh
    poetry run vaccine-feed-ingest/run.py all-stages
    ```
