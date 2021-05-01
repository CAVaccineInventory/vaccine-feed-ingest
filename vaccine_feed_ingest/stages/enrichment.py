"""Method for enriching location records after that are normalized"""
import logging
import pathlib

import pydantic
from vaccine_feed_ingest_schema import location

from . import outputs
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = logging.getLogger("enrichment")


def enrich_locations(input_dir: pathlib.Path, output_dir: pathlib.Path) -> bool:
    """Enrich locations in normalized input_dir and write them to output_dir"""
    enriched_locations = []

    for filepath in outputs.iter_data_paths(
        input_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]
    ):
        with filepath.open() as src_file:
            for line in src_file:
                try:
                    normalized_location = location.NormalizedLocation.parse_raw(line)
                except pydantic.ValidationError as e:
                    logger.warning(
                        "Skipping source location because it is invalid: %s\n%s",
                        line,
                        str(e),
                    )
                    continue

                enriched_location = _process_location(normalized_location)

                if not enriched_location:
                    continue

                enriched_locations.append(enriched_location)

    if not enriched_locations:
        return False

    suffix = STAGE_OUTPUT_SUFFIX[PipelineStage.ENRICH]
    dst_filepath = output_dir / f"locations{suffix}"

    with dst_filepath.open("w") as dst_file:
        for loc in enriched_locations:
            dst_file.write(loc.json())
            dst_file.write("\n")

    return True


def _process_location(loc: location.NormalizedLocation) -> location.NormalizedLocation:
    """Run throuch all of the methods to enrich the location"""
    return loc
