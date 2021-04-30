"""Shared constants and methods for running ingest"""

import enum
import pathlib

# Base directory that stores the code for each site runner
RUNNERS_DIR = pathlib.Path(__file__).parent.parent / "runners"


@enum.unique
class PipelineStage(str, enum.Enum):
    """Stages of ingestion pipeline."""

    FETCH = "fetch"
    PARSE = "parse"
    NORMALIZE = "normalize"
    ENRICH = "enrich"
    LOAD_TO_VIAL = "load-to-vial"


# Root name for command or config to run for each stage e.g. fetch.py
STAGE_CMD_NAME = {
    PipelineStage.FETCH: "fetch",
    PipelineStage.PARSE: "parse",
    PipelineStage.NORMALIZE: "normalize",
}


# Directory name for where to store data for each stage
STAGE_OUTPUT_NAME = {
    PipelineStage.FETCH: "raw",
    PipelineStage.PARSE: "parsed",
    PipelineStage.NORMALIZE: "normalized",
    PipelineStage.ENRICH: "enriched",
}


# Directory name for where to store data for each stage
STAGE_OUTPUT_SUFFIX = {
    PipelineStage.PARSE: ".parsed.ndjson",
    PipelineStage.NORMALIZE: ".normalized.ndjson",
    PipelineStage.ENRICH: ".enriched.ndjson",
}
