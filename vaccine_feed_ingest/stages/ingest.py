"""Code for running ingestion stage"""

import json
import pathlib
import subprocess
import tempfile
from subprocess import CalledProcessError
from typing import Collection, Optional

import orjson
import pydantic
from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils.log import getLogger

from ..utils.validation import VACCINATE_THE_STATES_BOUNDARY
from . import caching, enrichment, outputs, site
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = getLogger(__file__)


MAX_NORMALIZED_RECORD_SIZE = 15_000  # Maximum record size of 15KB for normalized reords


def run_fetch(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
    dry_run: bool = False,
    fail_on_runner_error: bool = True,
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

        try:
            subprocess.run(
                [str(fetch_path), str(fetch_output_dir), str(yml_path)], check=True
            )
        except CalledProcessError as e:
            if fail_on_runner_error:
                raise e
            logger.error("Subprocess errored, stage will be skipped: %s", e)
            return False

        if not outputs.data_exists(fetch_output_dir):
            msg = f"{fetch_path.name} for {site_dir.name} returned no data files."
            if fail_on_runner_error:
                raise NotImplementedError(msg)
            logger.warning(msg)
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
    fail_on_runner_error: bool = True,
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

        try:
            subprocess.run(
                [
                    str(parse_path),
                    str(parse_output_dir),
                    str(parse_input_dir),
                    str(yml_path),
                ],
                check=True,
            )
        except CalledProcessError as e:
            if fail_on_runner_error:
                raise e
            logger.error("Subprocess errored, stage will be skipped: %s", e)
            return False

        if not outputs.data_exists(
            parse_output_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE]
        ):
            msg = f"{parse_path.name} for {site_dir.name} returned no data files with expected extension {STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE]}."
            if fail_on_runner_error:
                raise NotImplementedError(msg)
            logger.warning(msg)
            return False

        if validate:
            if not _validate_parsed(parse_output_dir):
                msg = f"{parse_path.name} for {site_dir.name} returned invalid ndjson files."
                if fail_on_runner_error:
                    raise TypeError(msg)
                logger.warning(msg)
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
    fail_on_runner_error: bool = True,
) -> bool:
    normalize_path, yml_path = site.resolve_executable(
        site_dir, PipelineStage.NORMALIZE
    )
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

        try:
            if yml_path:
                subprocess.run(
                    [
                        str(normalize_path),
                        normalize_output_dir,
                        normalize_input_dir,
                        str(yml_path),
                    ],
                    check=True,
                )
            else:
                subprocess.run(
                    [str(normalize_path), normalize_output_dir, normalize_input_dir],
                    check=True,
                )
        except CalledProcessError as e:
            if fail_on_runner_error:
                raise e
            logger.error("Subprocess errored, stage will be skipped: %s", e)
            return False

        if not outputs.data_exists(
            normalize_output_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]
        ):
            msg = f"{normalize_path.name} for {site_dir.name} returned no data files with expected extension {STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]}."
            if fail_on_runner_error:
                raise NotImplementedError(msg)
            logger.warning(msg)
            return False

        if validate:
            if not _validate_normalized(normalize_output_dir):
                msg = f"{normalize_path.name} for {site_dir.name} returned invalid source location ndjson files."
                if fail_on_runner_error:
                    raise TypeError(msg)
                logger.warning(msg)
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


def run_enrich(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
    enable_apicache: bool = True,
    enrich_apis: Optional[Collection[str]] = None,
    geocodio_apikey: Optional[str] = None,
    placekey_apikey: Optional[str] = None,
    dry_run: bool = False,
) -> bool:
    normalize_run_dir = outputs.find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.NORMALIZE
    )
    if not normalize_run_dir:
        logger.warning(
            "Skipping enrich for %s because there is no data from normalize stage",
            site_dir.name,
        )
        return False

    if not outputs.data_exists(
        normalize_run_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]
    ):
        logger.warning(
            "No normalize data available to enrich for %s.",
            f"{site_dir.parent.name}/{site_dir.name}",
        )
        return False

    with tempfile.TemporaryDirectory(
        f"_enrich_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        enrich_output_dir = tmp_dir / "output"
        enrich_input_dir = tmp_dir / "input"

        enrich_output_dir.mkdir(parents=True, exist_ok=True)
        enrich_input_dir.mkdir(parents=True, exist_ok=True)

        outputs.copy_files(normalize_run_dir, enrich_input_dir)

        logger.info(
            "Enriching %s/%s and saving enriched output to %s",
            site_dir.parent.name,
            site_dir.name,
            enrich_output_dir,
        )

        success = None
        if enable_apicache and enrich_apis:
            with caching.api_cache_for_stage(
                output_dir, site_dir, PipelineStage.ENRICH
            ) as api_cache:
                success = enrichment.enrich_locations(
                    enrich_input_dir,
                    enrich_output_dir,
                    api_cache=api_cache,
                    enrich_apis=enrich_apis,
                    geocodio_apikey=geocodio_apikey,
                    placekey_apikey=placekey_apikey,
                )
        else:
            success = enrichment.enrich_locations(enrich_input_dir, enrich_output_dir)

        if not success:
            logger.error(
                "Enrichment failed for %s.", f"{site_dir.parent.name}/{site_dir.name}"
            )
            return False

        if not dry_run:
            enrich_run_dir = outputs.generate_run_dir(
                output_dir,
                site_dir.parent.name,
                site_dir.name,
                PipelineStage.ENRICH,
                timestamp,
            )

            logger.info(
                "Copying files from %s to %s", enrich_output_dir, enrich_run_dir
            )

            outputs.copy_files(enrich_output_dir, enrich_run_dir)

    return True


def _validate_parsed(output_dir: pathlib.Path) -> bool:
    """Validate output files are valid ndjson records."""
    for filepath in outputs.iter_data_paths(
        output_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.PARSE]
    ):
        with filepath.open(mode="rb") as ndjson_file:
            for line_no, content in enumerate(ndjson_file, start=1):
                try:
                    orjson.loads(content)
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
        with filepath.open(mode="rb") as ndjson_file:
            for line_no, content in enumerate(ndjson_file, start=1):
                if len(content) > MAX_NORMALIZED_RECORD_SIZE:
                    logger.warning(
                        "Source location too large to process in %s at line %d: %s",
                        filepath,
                        line_no,
                        content[:100],
                    )
                    return False

                try:
                    content_dict = orjson.loads(content)
                except json.JSONDecodeError:
                    logger.warning(
                        "Invalid json record in %s at line %d: %s",
                        filepath,
                        line_no,
                        content,
                    )
                    return False

                try:
                    normalized_location = location.NormalizedLocation.parse_obj(
                        content_dict
                    )
                except pydantic.ValidationError as e:
                    logger.warning(
                        "Invalid source location in %s at line %d: %s\n%s",
                        filepath,
                        line_no,
                        content[:100],
                        str(e),
                    )
                    return False

                if normalized_location.location:
                    if not VACCINATE_THE_STATES_BOUNDARY.contains(
                        normalized_location.location
                    ):
                        logger.warning(
                            "Invalid latitude or longitude in %s at line %d: %s is outside approved bounds (%s)",
                            filepath,
                            line_no,
                            normalized_location.location,
                            VACCINATE_THE_STATES_BOUNDARY,
                        )
                        return False

    return True
