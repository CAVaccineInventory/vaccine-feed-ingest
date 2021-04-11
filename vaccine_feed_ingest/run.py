#!/usr/bin/env python

"""
Entry point for running vaccine feed runners
"""
import datetime
import enum
import logging
import os
import pathlib
import subprocess
import tempfile
from typing import Iterator, Optional, Sequence

import click
import dotenv
import pathy

RUNNERS_DIR = pathlib.Path(__file__).parent / "runners"


@enum.unique
class PipelineStage(str, enum.Enum):
    """Stages of a pipeline to run."""

    FETCH = "fetch"
    PARSE = "parse"
    NORMALIZE = "normalize"


STAGE_CMD_NAME = {
    PipelineStage.FETCH: "fetch",
    PipelineStage.PARSE: "parse",
    PipelineStage.NORMALIZE: "normalize",
}


STAGE_OUTPUT_NAME = {
    PipelineStage.FETCH: "raw",
    PipelineStage.PARSE: "parsed",
    PipelineStage.NORMALIZE: "normalized",
}


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("ingest")


def _pathy_data_path(ctx, param, value):
    try:
        return pathy.Pathy.fluid(value)
    except (TypeError, ValueError):
        raise click.BadParameter("Data path needs to be a local or GCS file path.")


def _get_site_dirs_for_state(state: Optional[str] = None) -> Iterator[pathlib.Path]:
    """Return an iterator of site directory paths"""
    for state_dir in RUNNERS_DIR.iterdir():
        # Ignore private directories, in this case the _template directory
        if state_dir.name.startswith("_"):
            continue

        if state and state_dir.name.lower() != state.lower():
            continue

        yield from state_dir.iterdir()


def _get_site_dir(site: str) -> Optional[pathlib.Path]:
    """Return a site directory path, if it exists"""
    site_dir = RUNNERS_DIR / site

    if not site_dir.exists():
        return None

    return site_dir


def _get_site_dirs(
    state: Optional[str], sites: Optional[Sequence[str]]
) -> Iterator[pathlib.Path]:
    """Return a site directory path, if it exists"""
    if state is not None:
        yield from _get_site_dirs_for_state(state)

    elif sites is not None:
        for site in sites:
            site_dir = _get_site_dir(site)
            if not site_dir:
                continue

            yield site_dir


def _find_executeable(
    site_dir: pathlib.Path,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find executable. Logs an error and returs false if something is wrong."""
    cmd_name = STAGE_CMD_NAME[stage]

    cmds = list(site_dir.glob(f"{cmd_name}.*"))

    if not cmds:
        return None

    if len(cmds) > 1:
        logger.error(
            "Too many %s cmds in %s (%s).",
            cmd_name,
            str(site_dir),
            ", ".join([c.name for c in cmds]),
        )
        return None

    cmd = cmds[0]

    if not os.access(cmd, os.X_OK):
        logger.error("%s in %s is not marked as executable.", cmd.name, str(site_dir))
        return None

    return cmd


def _generate_run_timestamp() -> str:
    return datetime.datetime.now().replace(microsecond=0).isoformat()


def _find_all_run_dirs(
    base_output_dir: pathlib.Path,
    site: str,
    stage: PipelineStage,
) -> Iterator[pathlib.Path]:
    """Find latest stage output path"""
    stage_dir = base_output_dir / site / STAGE_OUTPUT_NAME[stage]

    for run_dir in sorted(stage_dir.iterdir(), reverse=True):
        if run_dir.name.startswith("_"):
            continue

        if run_dir.name.startswith("."):
            continue

        yield run_dir


def _find_latest_run_dir(
    base_output_dir: pathlib.Path,
    site: str,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find latest stage output path"""
    return next(_find_all_run_dirs(base_output_dir, site, stage), None)


def _generate_run_dir(
    base_output_dir: pathlib.Path,
    site: str,
    stage: PipelineStage,
    timestamp: str,
) -> pathlib.Path:
    """Generate output path for a pipeline stage."""
    run_dir = base_output_dir / site / STAGE_OUTPUT_NAME[stage] / timestamp

    run_dir.mkdir(parents=True, exist_ok=True)

    return run_dir


def _copy_files(src_dir: pathlib.Path, dst_dir: pathlib.Path) -> None:
    for filepath in src_dir.iterdir():
        if filepath.name.startswith("_") or filepath.name.startswith("."):
            continue

        with filepath.open("rb") as src_file:
            with (dst_dir / filepath.name).open("wb") as dst_file:
                for content in src_file:
                    dst_file.write(content)


def _run_fetch(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
) -> None:
    fetch_path = _find_executeable(site_dir, PipelineStage.FETCH)
    if not fetch_path:
        logger.info("No fetch cmd in %s to run.", str(site_dir))
        return

    fetch_run_dir = _generate_run_dir(
        output_dir, site_dir.name, PipelineStage.FETCH, timestamp
    )

    with tempfile.TemporaryDirectory(f"_fetch_{site_dir.name}") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)
        fetch_output_dir = tmp_dir / "output"
        fetch_output_dir.mkdir(parents=True, exist_ok=True)

        subprocess.run([str(fetch_path), str(fetch_output_dir)], check=True)

        _copy_files(fetch_output_dir, fetch_run_dir)


def _run_parse(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
) -> None:
    parse_path = _find_executeable(site_dir, PipelineStage.PARSE)
    if not parse_path:
        logger.info("No parse cmd in %s to run.", str(site_dir))
        return

    fetch_run_dir = _find_latest_run_dir(output_dir, site_dir.name, PipelineStage.FETCH)
    if not fetch_run_dir:
        logger.warning("Skipping parse stage because there is no data from fetch stage")
        return

    parse_run_dir = _generate_run_dir(
        output_dir, site_dir.name, PipelineStage.PARSE, timestamp
    )

    with tempfile.TemporaryDirectory(f"_parse_{site_dir.name}") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        parse_output_dir = tmp_dir / "output"
        parse_input_dir = tmp_dir / "input"

        parse_output_dir.mkdir(parents=True, exist_ok=True)
        parse_input_dir.mkdir(parents=True, exist_ok=True)

        _copy_files(fetch_run_dir, parse_input_dir)

        subprocess.run(
            [str(parse_path), str(parse_output_dir), str(parse_input_dir)],
            check=True,
        )

        _copy_files(parse_output_dir, parse_run_dir)


def _run_normalize(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
) -> None:
    normalize_path = _find_executeable(site_dir, PipelineStage.NORMALIZE)
    if not normalize_path:
        logger.info("No normalize cmd in %s to run.", str(site_dir))
        return

    parse_run_dir = _find_latest_run_dir(output_dir, site_dir.name, PipelineStage.PARSE)
    if not parse_run_dir:
        logger.warning(
            "Skipping normalize stage because there is no data from parse stage"
        )
        return

    normalize_run_dir = _generate_run_dir(
        output_dir, site_dir.name, PipelineStage.NORMALIZE, timestamp
    )

    with tempfile.TemporaryDirectory(f"_normalize_{site_dir.name}") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        normalize_output_dir = tmp_dir / "output"
        normalize_input_dir = tmp_dir / "input"

        normalize_output_dir.mkdir(parents=True, exist_ok=True)
        normalize_input_dir.mkdir(parents=True, exist_ok=True)

        _copy_files(parse_run_dir, normalize_input_dir)

        subprocess.run(
            [str(normalize_path), normalize_output_dir, normalize_input_dir],
            check=True,
        )

        _copy_files(normalize_output_dir, normalize_run_dir)


@click.group()
def cli():
    """Run vaccine-feed-ingest commands"""
    pass


@cli.command()
@click.option("--state", "state", type=str)
def available_sites(state: Optional[str]) -> None:
    """Print list of available sites, optionally filtered by state"""

    for site_dir in _get_site_dirs_for_state(state):
        has_fetch = bool(_find_executeable(site_dir, PipelineStage.FETCH))
        has_parse = bool(_find_executeable(site_dir, PipelineStage.PARSE))
        has_normalize = bool(_find_executeable(site_dir, PipelineStage.NORMALIZE))

        print(
            site_dir.relative_to(RUNNERS_DIR),
            "fetch" if has_fetch else "no-fetch",
            "parse" if has_parse else "no-parse",
            "normalize" if has_normalize else "no-normalize",
        )


@cli.command()
@click.option("--state", "state", type=str)
@click.option(
    "--output-dir", "output_dir", type=str, default="out", callback=_pathy_data_path
)
@click.argument("sites", nargs=-1, type=str)
def fetch(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run fetch process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        _run_fetch(site_dir, output_dir, timestamp)


@cli.command()
@click.option("--state", "state", type=str)
@click.option(
    "--output-dir", "output_dir", type=str, default="out", callback=_pathy_data_path
)
@click.argument("sites", nargs=-1, type=str)
def parse(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run parse process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        _run_parse(site_dir, output_dir, timestamp)


@cli.command()
@click.option("--state", "state", type=str)
@click.option(
    "--output-dir", "output_dir", type=str, default="out", callback=_pathy_data_path
)
@click.argument("sites", nargs=-1, type=str)
def normalize(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run normalize process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        _run_normalize(site_dir, output_dir, timestamp)


@cli.command()
@click.option("--state", "state", type=str)
@click.option(
    "--output-dir", "output_dir", type=str, default="out", callback=_pathy_data_path
)
@click.argument("sites", nargs=-1, type=str)
def all_stages(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run all stages in succession for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        _run_fetch(site_dir, output_dir, timestamp)
        _run_parse(site_dir, output_dir, timestamp)
        _run_normalize(site_dir, output_dir, timestamp)


@cli.command()
def version() -> None:
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    dotenv.load_dotenv()
    cli()
