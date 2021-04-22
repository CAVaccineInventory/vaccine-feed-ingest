#!/usr/bin/env python

"""
Entry point for running vaccine feed runners
"""
import datetime
import logging
import os
import pathlib
from typing import Callable, Optional, Sequence

import click
import dotenv
import pathy
from vaccine_feed_ingest import vial
from vaccine_feed_ingest.stages import common, ingest, load, site

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)


def _generate_run_timestamp() -> str:
    """Generate a timestam that will be recorded in the stage data output dirs"""
    return datetime.datetime.now().replace(microsecond=0).isoformat()


def _pathy_data_path(ctx, param, value):
    """Parameter callback for click to transform str into pathy local or GCS path."""
    try:
        return pathy.Pathy.fluid(value)
    except (TypeError, ValueError):
        raise click.BadParameter("Data path needs to be a local or GCS file path.")


# --- Common Click options --- #


def _output_dir_option() -> Callable:
    return click.option(
        "--output-dir",
        "output_dir",
        type=str,
        default=lambda: os.environ.get("OUTPUT_DIR", "out"),
        callback=_pathy_data_path,
    )


def _state_option() -> Callable:
    return click.option("--state", "state", type=str)


def _sites_argument() -> Callable:
    return click.argument("sites", nargs=-1, type=str)


@click.group()
def cli():
    """Run vaccine-feed-ingest commands"""
    dotenv.load_dotenv()


@cli.command()
@_state_option()
def available_sites(state: Optional[str]) -> None:
    """Print list of available sites, optionally filtered by state"""

    for site_dir in site.get_site_dirs_for_state(state):
        has_fetch = bool(site.find_executeable(site_dir, common.PipelineStage.FETCH))
        has_parse = bool(site.find_executeable(site_dir, common.PipelineStage.PARSE))
        has_normalize = bool(
            site.find_executeable(site_dir, common.PipelineStage.NORMALIZE)
        )

        print(
            site_dir.relative_to(common.RUNNERS_DIR),
            "fetch" if has_fetch else "no-fetch",
            "parse" if has_parse else "no-parse",
            "normalize" if has_normalize else "no-normalize",
        )


@cli.command()
@_state_option()
@_output_dir_option()
@_sites_argument()
def fetch(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run fetch process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_fetch(site_dir, output_dir, timestamp)


@cli.command()
@_state_option()
@_output_dir_option()
@_sites_argument()
def parse(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run parse process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_parse(site_dir, output_dir, timestamp)


@cli.command()
@_state_option()
@_output_dir_option()
@_sites_argument()
def normalize(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run normalize process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_normalize(site_dir, output_dir, timestamp)


@cli.command()
@_state_option()
@_output_dir_option()
@_sites_argument()
def all_stages(
    state: Optional[str], output_dir: pathlib.Path, sites: Optional[Sequence[str]]
) -> None:
    """Run all stages in succession for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        fetch_success = ingest.run_fetch(site_dir, output_dir, timestamp)

        if not fetch_success:
            continue

        parse_success = ingest.run_parse(site_dir, output_dir, timestamp)

        if not parse_success:
            continue

        ingest.run_normalize(site_dir, output_dir, timestamp)


@cli.command()
@click.option(
    "--vial-server",
    "vial_server",
    type=str,
    default=lambda: os.environ.get(
        "VIAL_SERVER", "https://vial-staging.calltheshots.us"
    ),
)
@click.option(
    "--vial-apikey",
    "vial_apikey",
    type=str,
    default=lambda: os.environ.get("VIAL_APIKEY", ""),
)
@_state_option()
@_output_dir_option()
@_sites_argument()
@click.option("--match/--no-match", default=True)
def load_to_vial(
    vial_server: str,
    vial_apikey: str,
    state: Optional[str],
    output_dir: pathlib.Path,
    sites: Optional[Sequence[str]],
    match: bool,
) -> None:
    """Load specified sites to vial server."""
    site_dirs = site.get_site_dirs(state, sites)

    with vial.vial_client(vial_server, vial_apikey) as vial_http:
        import_run_id = vial.start_import_run(vial_http)

        for site_dir in site_dirs:
            locations = None
            if match:
                locations = vial.retrieve_existing_locations(vial_http)

            load.run_load_to_vial(
                vial_http,
                site_dir,
                output_dir,
                import_run_id,
                locations,
            )


@cli.command()
def version() -> None:
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    cli()
