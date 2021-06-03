FROM python:3.9.2-buster
LABEL name=vaccine-feed-ingest

RUN useradd -m vaccine && mkdir vaccine-feed-ingest && chown vaccine:vaccine vaccine-feed-ingest

COPY ./ /vaccine-feed-ingest/

USER vaccine

WORKDIR /vaccine-feed-ingest

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
ENV PATH "/home/vaccine/.poetry/bin:$PATH"
RUN /home/vaccine/.poetry/bin/poetry config virtualenvs.create false && \
    /home/vaccine/.poetry/bin/poetry install --extras lint --no-interaction --no-ansi

CMD ["bash"]
