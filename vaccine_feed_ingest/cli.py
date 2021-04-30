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

from . import vial
from .stages import common, ingest, load, site

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)

# Collect locations that are within .6 degrees = 66.6 km = 41 mi
CANDIDATE_DEGREES_DISTANCE = 0.6


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


def _dry_run_option() -> Callable:
    return click.option("--dry-run/--no-dry-run", type=bool, default=False)


def _validate_option() -> Callable:
    return click.option(
        "--validate/--no-validate",
        type=bool,
        default=lambda: os.environ.get("ENABLE_VALIDATE", "true").lower() == "true",
    )


def _state_option() -> Callable:
    return click.option("--state", "state", type=str)


def _sites_argument() -> Callable:
    return click.argument("sites", nargs=-1, type=str)


def _vial_server_option() -> Callable:
    return click.option(
        "--vial-server",
        "vial_server",
        type=str,
        default=lambda: os.environ.get(
            "VIAL_SERVER", "https://vial-staging.calltheshots.us"
        ),
    )


def _vial_apikey_option() -> Callable:
    return click.option(
        "--vial-apikey",
        "vial_apikey",
        type=str,
        default=lambda: os.environ.get("VIAL_APIKEY", ""),
    )


def _match_option() -> Callable:
    return click.option(
        "--match/--no-match",
        "enable_match",
        type=bool,
        default=lambda: os.environ.get("ENABLE_MATCH", "true").lower() == "true",
    )


def _create_option() -> Callable:
    return click.option(
        "--create/--no-create",
        "enable_create",
        type=bool,
        default=lambda: os.environ.get("ENABLE_CREATE", "false").lower() == "true",
    )


def _candidate_distance_option() -> Callable:
    return click.option(
        "--candidate-distance",
        "candidate_distance",
        type=float,
        default=CANDIDATE_DEGREES_DISTANCE,
    )


@click.group()
def cli():
    """Run vaccine-feed-ingest commands"""
    dotenv.load_dotenv()


@cli.command()
@_state_option()
def available_sites(state: Optional[str]) -> None:
    """Print list of available sites, optionally filtered by state"""

    for site_dir in site.get_site_dirs_for_state(state):
        has_fetch = _compute_has_fetch(site_dir)
        has_parse = _compute_has_parse(site_dir)
        has_normalize = bool(
            site.find_executeable(site_dir, common.PipelineStage.NORMALIZE)
        )

        print(
            site_dir.relative_to(common.RUNNERS_DIR),
            "fetch" if has_fetch else "no-fetch",
            "parse" if has_parse else "no-parse",
            "normalize" if has_normalize else "no-normalize",
        )


def _compute_has_fetch(site_dir: pathlib.Path) -> bool:
    exec_path, _ = site.resolve_executable(site_dir, common.PipelineStage.FETCH)
    return bool(exec_path)


def _compute_has_parse(site_dir: pathlib.Path) -> bool:
    exec_path, _ = site.resolve_executable(site_dir, common.PipelineStage.PARSE)
    return bool(exec_path)


@cli.command()
@_sites_argument()
@_state_option()
@_output_dir_option()
@_dry_run_option()
def fetch(
    sites: Optional[Sequence[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
) -> None:
    """Run fetch process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_fetch(site_dir, output_dir, timestamp, dry_run)


@cli.command()
@_sites_argument()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_validate_option()
def parse(
    sites: Optional[Sequence[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    validate: bool,
) -> None:
    """Run parse process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_parse(site_dir, output_dir, timestamp, validate, dry_run)


@cli.command()
@_sites_argument()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_validate_option()
def normalize(
    sites: Optional[Sequence[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    validate: bool,
) -> None:
    """Run normalize process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_normalize(site_dir, output_dir, timestamp, validate, dry_run)


@cli.command()
@_sites_argument()
@_state_option()
@_output_dir_option()
def all_stages(
    sites: Optional[Sequence[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
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
@_sites_argument()
@_state_option()
@_output_dir_option()
@_dry_run_option()
def enrich(
    sites: Optional[Sequence[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
) -> None:
    """Run enrich process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_enrich(site_dir, output_dir, timestamp, dry_run)


@cli.command()
@_sites_argument()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_vial_server_option()
@_vial_apikey_option()
@_match_option()
@_create_option()
@_candidate_distance_option()
def load_to_vial(
    sites: Optional[Sequence[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    vial_server: str,
    vial_apikey: str,
    enable_match: bool,
    enable_create: bool,
    candidate_distance: float,
) -> None:
    """Load specified sites to vial server."""
    site_dirs = site.get_site_dirs(state, sites)

    with vial.vial_client(vial_server, vial_apikey) as vial_http:
        import_run_id = vial.start_import_run(vial_http)

        if enable_match or enable_create:
            locations = vial.retrieve_existing_locations_as_index(vial_http)

        for site_dir in site_dirs:
            imported_locations = load.run_load_to_vial(
                vial_http,
                site_dir,
                output_dir,
                import_run_id,
                locations,
                enable_match=enable_match,
                enable_create=enable_create,
                candidate_distance=candidate_distance,
                dry_run=dry_run,
            )

            # If data was loaded then refresh existing locations
            if locations is not None and imported_locations:
                source_ids = [
                    loc.source_uid
                    for loc in imported_locations
                    if loc.match and loc.match.action == "new"
                ]

                if source_ids:
                    vial.update_existing_locations(vial_http, locations, source_ids)


@cli.command()
def version() -> None:
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    cli()
