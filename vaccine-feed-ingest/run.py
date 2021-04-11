#!/usr/bin/env python

"""
Entry point for running vaccine feed runners
"""
import logging
import os
import pathlib
import subprocess
import tempfile
from typing import Iterator, Optional, Sequence

import click
import dotenv

RUNNERS_DIR = pathlib.Path(__file__).parent / "runners"

FETCH_CMD = "fetch"
PARSE_CMD = "parse"
NORMALIZE_CMD = "normalize"


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("ingest")


def _get_site_dirs(state: Optional[str] = None) -> Iterator[pathlib.Path]:
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


def _find_executeable(site_dir: pathlib.Path, cmd_name: str) -> Optional[pathlib.Path]:
    """Find executable. Logs an error and returs false if something is wrong."""
    cmds = list(site_dir.glob(f"{cmd_name}.*"))

    if not cmds:
        logger.info("No %s cmd in %s to run.", cmd_name, str(site_dir))
        return None

    if len(cmds) > 1:
        logger.error(
            "Too many %s cmds in %s (%s).", cmd_name, str(site_dir), ", ".join(cmds)
        )
        return None

    cmd = cmds[0]

    if not os.access(cmd, os.X_OK):
        logger.error("%s in %s is not marked as executable.", cmd.name, str(site_dir))
        return None

    return cmd


def _run_fetch(site_dir: pathlib.Path) -> None:
    fetch_path = _find_executeable(site_dir, FETCH_CMD)
    if not fetch_path:
        return

    with tempfile.TemporaryDirectory(f"_fetch_{site_dir.name}") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        subprocess.run([str(fetch_path), str(tmp_dir)], check=True)


def _run_parse(site_dir: pathlib.Path) -> None:
    parse_path = _find_executeable(site_dir, PARSE_CMD)
    if not parse_path:
        return

    with tempfile.TemporaryDirectory(f"_parse_{site_dir.name}") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        subprocess.run(
            [str(parse_path), str(tmp_dir / "output"), str(tmp_dir / "input")],
            check=True,
        )


def _run_normalize(site_dir: pathlib.Path) -> None:
    normalize_path = _find_executeable(site_dir, NORMALIZE_CMD)
    if not normalize_path:
        return

    with tempfile.TemporaryDirectory(f"_normalize_{site_dir.name}") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        subprocess.run(
            [str(normalize_path), str(tmp_dir / "output"), str(tmp_dir / "input")],
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

    for site_dir in _get_site_dirs(state):
        has_fetch = (site_dir / FETCH_CMD).exists()
        has_parse = (site_dir / PARSE_CMD).exists()
        has_normalize = (site_dir / NORMALIZE_CMD).exists()

        print(
            site_dir.relative_to(RUNNERS_DIR),
            "fetch" if has_fetch else "no-fetch",
            "parse" if has_parse else "no-parse",
            "normalize" if has_normalize else "no-normalize",
        )


@cli.command()
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def fetch(state: Optional[str], sites: Optional[Sequence[str]]):
    """Run fetch process for specified sites."""
    if not sites:
        site_dirs = list(_get_site_dirs(state))
    else:
        site_dirs = [_get_site_dir(site) for site in sites]

    for site_dir in site_dirs:
        _run_fetch(site_dir)


@cli.command()
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def parse(state: Optional[str], sites: Optional[Sequence[str]]):
    """Run parse process for specified sites."""
    if not sites:
        site_dirs = list(_get_site_dirs(state))
    else:
        site_dirs = [_get_site_dir(site) for site in sites]

    for site_dir in site_dirs:
        _run_parse(site_dir)


@cli.command()
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def normalize(state: Optional[str], sites: Optional[Sequence[str]]):
    """Run normalize process for specified sites."""
    if not sites:
        site_dirs = list(_get_site_dirs(state))
    else:
        site_dirs = [_get_site_dir(site) for site in sites]

    for site_dir in site_dirs:
        _run_normalize(site_dir)


@cli.command()
@click.option("--state", "state", type=str)
@click.argument("sites", nargs=-1, type=str)
def all_stages(state: Optional[str], sites: Optional[Sequence[str]]):
    """Run all stages in succession for specified sites."""
    if not sites:
        site_dirs = list(_get_site_dirs(state))
    else:
        site_dirs = [_get_site_dir(site) for site in sites]

    for site_dir in site_dirs:
        _run_fetch(site_dir)
        _run_parse(site_dir)
        _run_normalize(site_dir)


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    dotenv.load_dotenv()
    cli()
