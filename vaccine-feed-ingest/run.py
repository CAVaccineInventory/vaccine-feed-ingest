#!/usr/bin/env python

"""
Entry point for running vaccine feed runners
"""
import logging
import pathlib
import subprocess
from typing import Iterator, Optional, Sequence

import click
import dotenv

RUNNERS_DIR = pathlib.Path(__file__).parent / "runners"

FETCH_CMD = "fetch.sh"
PARSE_SH = "parse.sh"
PARSE_PY = "parse.py"
NORMALIZE_SH = "normalize.sh"
NORMALIZE_PY = "normalize.py"

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("ingest")


def _get_state_dirs(state: Optional[str] = None) -> Iterator[pathlib.Path]:
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

    if site_dir.exists():
        return site_dir


def _get_site_dirs(state: str, sites: str) -> Optional[pathlib.Path]:
    if not sites:
        site_dirs = list(_get_state_dirs(state))
    else:
        site_dirs = [_get_site_dir(site) for site in sites]

    return site_dirs


def _run_fetch(site_dir: pathlib.Path, output_path: pathlib.Path) -> None:
    fetch_path = site_dir / FETCH_CMD
    if not fetch_path.exists():
        return

    logger.info(f"Fetching data for {site_dir.name} into {output_path}")
    subprocess.run([str(fetch_path), str(output_path)], check=True)


def _run_parse(site_dir: pathlib.Path, output_path: pathlib.Path) -> None:
    """
    Execute either parse.sh or parse.py for a site.
    Create a locations.ndjson file in output_path containing one line per location.
    """

    parse_path = site_dir / PARSE_SH
    if not parse_path.exists():
        parse_path = site_dir / PARSE_PY
        if not parse_path.exists():
            return

    logger.info(f"Parsing data in {output_path} into locations.ndjson")
    subprocess.run(
        [str(parse_path), str(output_path), str(output_path / "locations.ndjson")],
        check=True,
    )


def _run_normalize(site_dir: pathlib.Path, output_path: pathlib.Path) -> None:
    """
    Execute either normalize.sh or normalize.py for a site.
    Parse a locations.ndjson file into a locations-normalized.ndjson file.
    """

    normalize_path = site_dir / NORMALIZE_SH
    if not normalize_path.exists():
        normalize_path = site_dir / NORMALIZE_PY
        if not normalize_path.exists():
            return

    ndjson_file = str(output_path / "locations.ndjson")
    normalized_file = str(output_path / "locations-normalized.ndjson")
    logger.info(f"Normalizing data in {ndjson_file} into locations-normalized.ndjson")
    subprocess.run(
        [str(normalize_path), ndjson_file, normalized_file],
        check=True,
    )


@click.group()
def cli():
    """Run vaccine-feed-ingest commands"""
    pass


@cli.command()
@click.option("--state", "state", type=str)
def available_sites(state: Optional[str]):
    """Print list of available sites, optionally filtered by state"""

    for site_dir in _get_state_dirs(state):
        has_fetch = (site_dir / FETCH_CMD).exists()
        has_parse = (site_dir / PARSE_SH).exists() or (site_dir / PARSE_PY).exists()
        has_normalize = (site_dir / NORMALIZE_SH).exists() or (
            site_dir / NORMALIZE_PY
        ).exists()

        print(
            site_dir.relative_to(RUNNERS_DIR),
            "fetch" if has_fetch else "no-fetch",
            "parse" if has_parse else "no-parse",
            "normalize" if has_normalize else "no-normalize",
        )


@cli.command()
@click.option("--output-dir", "output_dir", type=str, required=True)
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def fetch(output_dir: str, state: Optional[str], sites: Optional[Sequence[str]]):
    """Run fetch process for specified sites."""

    output_parent = pathlib.Path(output_dir)
    if not output_parent.exists():
        click.echo("The specified output directory does not exist!")
        return

    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        output_path = output_parent / site_dir.relative_to(RUNNERS_DIR)
        if not output_path.exists():
            output_path.mkdir(parents=True)
        _run_fetch(site_dir, output_path)


@cli.command()
@click.option("--output-dir", "output_dir", type=str, required=True)
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def parse(output_dir: str, state: Optional[str], sites: Optional[Sequence[str]]):
    """Run parse process for specified sites."""

    output_parent = pathlib.Path(output_dir)
    if not output_parent.exists():
        click.echo("The specified output directory does not exist!")
        return

    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        output_path = output_parent / site_dir.relative_to(RUNNERS_DIR)
        _run_parse(site_dir, output_path)


@cli.command()
@click.option("--output-dir", "output_dir", type=str, required=True)
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def normalize(output_dir: str, state: Optional[str], sites: Optional[Sequence[str]]):
    """Run normalize process for specified sites."""

    output_parent = pathlib.Path(output_dir)
    if not output_parent.exists():
        click.echo("The specified output directory does not exist!")
        return

    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        output_path = output_parent / site_dir.relative_to(RUNNERS_DIR)
        _run_normalize(site_dir, output_path)


@cli.command()
@click.option("--output-dir", "output_dir", type=str, required=True)
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def all_stages(output_dir: str, state: Optional[str], sites: Optional[Sequence[str]]):
    """Run all stages in succession for specified sites."""

    output_parent = pathlib.Path(output_dir)
    if not output_parent.exists():
        click.echo("The specified output directory does not exist!")
        return

    site_dirs = _get_site_dirs(state, sites)

    for site_dir in site_dirs:
        output_path = output_parent / site_dir.relative_to(RUNNERS_DIR)
        if not output_path.exists():
            output_path.mkdir(parents=True)
        _run_fetch(site_dir, output_path)
        _run_parse(site_dir, output_path)
        _run_normalize(site_dir, output_path)


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    dotenv.load_dotenv()
    cli()
