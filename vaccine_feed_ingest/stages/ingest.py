"""Code for running ingestion stage"""

import json
import logging
import pathlib
import subprocess
import tempfile

import pydantic
from vaccine_feed_ingest.schema import schema

from . import outputs, site
from .common import RUNNERS_DIR, PipelineStage, STAGE_OUTPUT_SUFFIX

logger = logging.getLogger("ingest")


def run_fetch(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
    dry_run: bool = False,
) -> bool:
    fetch_path = site.find_executeable(site_dir, PipelineStage.FETCH)

    yml_path = None
    if not fetch_path:
        yml_path = site.find_yml(site_dir, PipelineStage.FETCH)

        if not yml_path:
            logger.info("No fetch cmd or .yml config for %s to run.", site_dir.name)
            return False

        fetch_path = site.find_executeable(
            RUNNERS_DIR.joinpath("_shared"), PipelineStage.FETCH
        )

        if not fetch_path:
            logger.info(
                "Missing shapred executable to run for yml in %s.", site_dir.name
            )
            return False

    with tempfile.TemporaryDirectory(
        f"_fetch_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)
        fetch_output_dir = tmp_dir / "output"
        fetch_output_dir.mkdir(parents=True, exist_ok=True)

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

            outputs.copy_files(fetch_output_dir, fetch_run_dir)

    return True


def run_parse(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
    validate: bool = True,
    dry_run: bool = False,
) -> bool:
    parse_path = site.find_executeable(site_dir, PipelineStage.PARSE)
    yml_path = None
    if not parse_path:
        yml_path = site.find_yml(site_dir, PipelineStage.PARSE)

        if not yml_path:
            logger.info("No parse cmd or .yml config for %s to run.", site_dir.name)
            return False

        parse_path = site.find_executeable(
            RUNNERS_DIR.joinpath("_shared"), PipelineStage.PARSE
        )

        if not parse_path:
            logger.info(
                "Missing shapred executable to run for yml in %s.", site_dir.name
            )
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
                "%s for %s returned no data files.", parse_path.name, site_dir.name
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

    if not outputs.data_exists(parse_run_dir):
        logger.warning("No parse data available to normalize for %s.", site_dir.name)
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

        subprocess.run(
            [str(normalize_path), normalize_output_dir, normalize_input_dir],
            check=True,
        )

        if not outputs.data_exists(normalize_output_dir):
            logger.warning(
                "%s for %s returned no data files.", normalize_path.name, site_dir.name
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
                    schema.NormalizedLocation.parse_raw(content)
                except pydantic.ValidationError as e:
                    logger.warning(
                        "Invalid source location in %s at line %d: %s\n%s",
                        filepath,
                        line_no,
                        content,
                        str(e),
                    )
                    return False

    return True
