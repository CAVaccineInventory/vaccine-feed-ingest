"""Code for running ingestion stage"""

import json
import logging
import pathlib
import subprocess
import tempfile

import pydantic
from vaccine_feed_ingest_schema import location

from ..utils.validation import BOUNDING_BOX, BOUNDING_BOX_GUAM
from . import outputs, site
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = logging.getLogger("ingest")

APPROVED_BOUNDS = [BOUNDING_BOX, BOUNDING_BOX_GUAM]


def run_fetch(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
    dry_run: bool = False,
) -> bool:
    fetch_path, yml_path = site.resolve_executable(site_dir, PipelineStage.FETCH)
    if not fetch_path:
        log_msg = (
            "Missing shared executable to run for yml in %s."
            if yml_path
            else "No fetch cmd or .yml config for %s to run."
        )
        logger.info(log_msg, site_dir.name)
        return False

    with tempfile.TemporaryDirectory(
        f"_fetch_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)
        fetch_output_dir = tmp_dir / "output"
        fetch_output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Fetching %s/%s and saving fetched output to %s",
            site_dir.parent.name,
            site_dir.name,
            fetch_output_dir,
        )

        subprocess.run(
            [str(fetch_path), str(fetch_output_dir), str(yml_path)], check=True
        )

        if not outputs.data_exists(fetch_output_dir):
            logger.warning(
                "%s for %s returned no data files.", fetch_path.name, site_dir.name
            )
            return False

        if not dry_run:
            fetch_run_dir = outputs.generate_run_dir(
                output_dir,
                site_dir.parent.name,
                site_dir.name,
                PipelineStage.FETCH,
                timestamp,
            )

            logger.info("Copying files from %s to %s", fetch_output_dir, fetch_run_dir)

            outputs.copy_files(fetch_output_dir, fetch_run_dir)

    return True


def run_parse(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
    validate: bool = True,
    dry_run: bool = False,
) -> bool:
    parse_path, yml_path = site.resolve_executable(site_dir, PipelineStage.PARSE)
    if not parse_path:
        log_msg = (
            "Missing shared executable to run for yml in %s."
            if yml_path
            else "No parse cmd or .yml config for %s to run."
        )
        logger.info(log_msg, site_dir.name)
        return False

    fetch_run_dir = outputs.find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.FETCH
    )
    if not fetch_run_dir:
        logger.warning(
            "Skipping parse stage for %s because there is no data from fetch stage",
            site_dir.name,
        )
        return False

    if not outputs.data_exists(fetch_run_dir):
        logger.warning("No fetch data available to parse for %s.", site_dir.name)
        return False

    with tempfile.TemporaryDirectory(
        f"_parse_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        parse_output_dir = tmp_dir / "output"
        parse_input_dir = tmp_dir / "input"

        parse_output_dir.mkdir(parents=True, exist_ok=True)
        parse_input_dir.mkdir(parents=True, exist_ok=True)

        outputs.copy_files(fetch_run_dir, parse_input_dir)

        logger.info(
            "Parsing %s/%s and saving parsed output to %s",
            site_dir.parent.name,
            site_dir.name,
            parse_output_dir,
        )

        subprocess.run(
            [
                str(parse_path),
                str(parse_output_dir),
                str(parse_input_dir),
                str(yml_path),
            ],
            check=True,
        )

        if not outputs.data_exists(
            parse_output_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE]
        ):
            logger.warning(
                "%s for %s returned no data files with expected extension %s.",
                parse_path.name,
                site_dir.name,
                STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE],
            )
            return False

        if validate:
            if not _validate_parsed(parse_output_dir):
                logger.warning(
                    "%s for %s returned invalid ndjson files.",
                    parse_path.name,
                    site_dir.name,
                )
                return False

        if not dry_run:
            parse_run_dir = outputs.generate_run_dir(
                output_dir,
                site_dir.parent.name,
                site_dir.name,
                PipelineStage.PARSE,
                timestamp,
            )

            logger.info("Copying files from %s to %s", parse_output_dir, parse_run_dir)

            outputs.copy_files(parse_output_dir, parse_run_dir)

    return True


def run_normalize(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
    validate: bool = True,
    dry_run: bool = False,
) -> bool:
    normalize_path = site.find_executeable(site_dir, PipelineStage.NORMALIZE)
    if not normalize_path:
        logger.info("No normalize cmd for %s to run.", site_dir.name)
        return False

    parse_run_dir = outputs.find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.PARSE
    )
    if not parse_run_dir:
        logger.warning(
            "Skipping normalize stage for %s because there is no data from parse stage",
            site_dir.name,
        )
        return False

    if not outputs.data_exists(
        parse_run_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE]
    ):
        logger.warning(
            "No parse data available to normalize for %s with extension %s.",
            site_dir.name,
            STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE],
        )
        return False

    with tempfile.TemporaryDirectory(
        f"_normalize_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        normalize_output_dir = tmp_dir / "output"
        normalize_input_dir = tmp_dir / "input"

        normalize_output_dir.mkdir(parents=True, exist_ok=True)
        normalize_input_dir.mkdir(parents=True, exist_ok=True)

        outputs.copy_files(parse_run_dir, normalize_input_dir)

        logger.info(
            "Normalizing %s/%s and saving normalized output to %s",
            site_dir.parent.name,
            site_dir.name,
            normalize_output_dir,
        )

        subprocess.run(
            [str(normalize_path), normalize_output_dir, normalize_input_dir],
            check=True,
        )

        if not outputs.data_exists(
            normalize_output_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]
        ):
            logger.warning(
                "%s for %s returned no data files with expected extension %s.",
                normalize_path.name,
                site_dir.name,
                STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE],
            )
            return False

        if validate:
            if not _validate_normalized(normalize_output_dir):
                logger.warning(
                    "%s for %s returned invalid source location ndjson files.",
                    normalize_path.name,
                    site_dir.name,
                )
                return False

        if not dry_run:
            normalize_run_dir = outputs.generate_run_dir(
                output_dir,
                site_dir.parent.name,
                site_dir.name,
                PipelineStage.NORMALIZE,
                timestamp,
            )

            logger.info(
                "Copying files from %s to %s", normalize_output_dir, normalize_run_dir
            )

            outputs.copy_files(normalize_output_dir, normalize_run_dir)

    return True


def _validate_parsed(output_dir: pathlib.Path) -> bool:
    """Validate output files are valid ndjson records."""
    for filepath in outputs.iter_data_paths(
        output_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE]
    ):
        with filepath.open() as ndjson_file:
            for line_no, content in enumerate(ndjson_file):
                try:
                    json.loads(content)
                except json.JSONDecodeError:
                    logger.warning(
                        "Invalid json record in %s at line %d: %s",
                        filepath,
                        line_no,
                        content,
                    )
                    return False

    return True


def _validate_normalized(output_dir: pathlib.Path) -> bool:
    """Validate output files are valid normalized locations."""
    for filepath in outputs.iter_data_paths(
        output_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]
    ):
        with filepath.open() as ndjson_file:
            for line_no, content in enumerate(ndjson_file):
                try:
                    normalized_location = location.NormalizedLocation.parse_raw(content)
                except pydantic.ValidationError as e:
                    logger.warning(
                        "Invalid source location in %s at line %d: %s\n%s",
                        filepath,
                        line_no,
                        content,
                        str(e),
                    )
                    return False

                if normalized_location.location:
                    result = validate_bounding_boxes(
                        normalized_location.location, APPROVED_BOUNDS
                    )

                    # if false, return false
                    if not result:
                        logger.warning(
                            "Invalid latitude or longitude in %s at line %d: %s is outside approved bounds (%s)",
                            filepath,
                            line_no,
                            normalized_location.location,
                            APPROVED_BOUNDS,
                        )
                        return False

    return True


def validate_bounding_boxes(location, bounding_boxes):
    results = []

    for boundingbox in bounding_boxes:
        if not boundingbox.latitude.contains(
            location.latitude
        ) or not boundingbox.longitude.contains(location.longitude):
            results.append(False)

        results.append(True)

    # only fail if all bounding boxes fail
    try:
        results.index(True)
    except ValueError:
        # if True is not in the list, then they all failed, so fail
        return False

    return True
