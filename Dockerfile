FROM python:3.9.2-buster
LABEL name=vaccine-feed-ingest

RUN apt-get update && apt-get install -y \
    libbz2-dev liblzma-dev libreadline-dev libsqlite3-dev

RUN useradd -m vaccine
RUN mkdir vaccine-feed-ingest && chown vaccine:vaccine vaccine-feed-ingest
USER vaccine

RUN git clone https://github.com/CAVaccineInventory/vaccine-feed-ingest.git

WORKDIR /vaccine-feed-ingest

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
ENV PATH "/home/vaccine/.poetry/bin:$PATH"
RUN /home/vaccine/.poetry/bin/poetry install --extras lint

CMD bash
