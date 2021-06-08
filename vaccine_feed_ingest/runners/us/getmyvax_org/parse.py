#!/usr/bin/env python3

import pathlib
import sys

import orjson

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


def _strip_unused_larged_fields(loc: dict) -> None:
    """Remove large fields that won't be used in normalize.

    This should be used rarely.
    """
    # Remove appointment availability slots
    if loc.get("availability") and "slots" in loc["availability"]:
        del loc["availability"]["slots"]

    # Remove appointment availability capacity
    if loc.get("availability") and "capacity" in loc["availability"]:
        del loc["availability"]["capacity"]


output_dir = pathlib.Path(sys.argv[1]) if len(sys.argv) >= 2 else None
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

input_dir = pathlib.Path(sys.argv[2]) if len(sys.argv) >= 3 else None
if input_dir is None:
    logger.error("Must pass an input_dir as second argument")
    sys.exit(1)

for in_filepath in input_dir.iterdir():
    if in_filepath.suffix != ".ndjson":
        continue

    logger.info(f"Parsing locations in {in_filepath.name}")

    with in_filepath.open("rb") as in_file:
        out_filepath = output_dir / f"{in_filepath.stem}.parsed.ndjson"
        with out_filepath.open("wb") as out_file:
            for line in in_file:
                loc = orjson.loads(line)

                _strip_unused_larged_fields(loc)

                out_file.write(orjson.dumps(loc, option=orjson.OPT_APPEND_NEWLINE))
