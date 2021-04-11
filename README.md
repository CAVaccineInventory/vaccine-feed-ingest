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

### Setup Environment for Ubuntu/Debian

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
