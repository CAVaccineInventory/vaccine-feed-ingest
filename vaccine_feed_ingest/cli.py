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
    if site.find_executeable(site_dir, common.PipelineStage.FETCH):
        return True
    if not site.find_yml(site_dir, common.PipelineStage.FETCH):
        return False
    return bool(
        site.find_executeable(
            common.RUNNERS_DIR.joinpath("_shared"), common.PipelineStage.FETCH
        )
    )


def _compute_has_parse(site_dir: pathlib.Path) -> bool:
    if site.find_executeable(site_dir, common.PipelineStage.PARSE):
        return True
    if not site.find_yml(site_dir, common.PipelineStage.PARSE):
        return False
    return bool(
        site.find_executeable(
            common.RUNNERS_DIR.joinpath("_shared"), common.PipelineStage.PARSE
        )
    )


@cli.command()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_sites_argument()
def fetch(
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    sites: Optional[Sequence[str]],
) -> None:
    """Run fetch process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_fetch(site_dir, output_dir, timestamp, dry_run)


@cli.command()
@_state_option()
@_output_dir_option()
@_validate_option()
@_dry_run_option()
@_sites_argument()
def parse(
    state: Optional[str],
    output_dir: pathlib.Path,
    validate: bool,
    dry_run: bool,
    sites: Optional[Sequence[str]],
) -> None:
    """Run parse process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_parse(site_dir, output_dir, timestamp, validate, dry_run)


@cli.command()
@_state_option()
@_output_dir_option()
@_validate_option()
@_dry_run_option()
@_sites_argument()
def normalize(
    state: Optional[str],
    output_dir: pathlib.Path,
    validate: bool,
    dry_run: bool,
    sites: Optional[Sequence[str]],
) -> None:
    """Run normalize process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        ingest.run_normalize(site_dir, output_dir, timestamp, validate, dry_run)


@cli.command()
@_state_option()
@_validate_option()
@_output_dir_option()
@_sites_argument()
def all_stages(
    state: Optional[str],
    validate: bool,
    output_dir: pathlib.Path,
    sites: Optional[Sequence[str]],
) -> None:
    """Run all stages in succession for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites)

    for site_dir in site_dirs:
        fetch_success = ingest.run_fetch(site_dir, output_dir, timestamp)

        if not fetch_success:
            continue

        parse_success = ingest.run_parse(site_dir, output_dir, timestamp, validate)

        if not parse_success:
            continue

        ingest.run_normalize(site_dir, output_dir, timestamp, validate)


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
@_dry_run_option()
@_sites_argument()
@click.option(
    "--match/--no-match",
    "enable_match",
    type=bool,
    default=lambda: os.environ.get("ENABLE_MATCH", "true").lower() == "true",
)
@click.option(
    "--create/--no-create",
    "enable_create",
    type=bool,
    default=lambda: os.environ.get("ENABLE_CREATE", "false").lower() == "true",
)
def load_to_vial(
    vial_server: str,
    vial_apikey: str,
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    sites: Optional[Sequence[str]],
    enable_match: bool,
    enable_create: bool,
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
