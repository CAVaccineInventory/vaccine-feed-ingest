#!/usr/bin/env python

"""
Entry point for running vaccine feed runners
"""
import datetime
import os
import pathlib
from typing import Callable, Collection, Dict, Optional, Sequence

import click
import dotenv
import pathy

from .stages import caching, common, ingest, load, site

# Collect locations that are within .6 degrees = 66.6 km = 41 mi
CANDIDATE_DEGREES_DISTANCE = 0.6

# Default import batch size to vial
IMPORT_BATCH_SIZE = 500


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


def _exclude_sites_option() -> Callable:
    return click.option(
        "--exclude-sites",
        type=str,
        default=lambda: os.environ.get("EXCLUDE_SITES", ""),
        callback=lambda ctx, param, value: set(
            [item.strip().lower() for item in value.split(",")]
        ),
    )


def _stages_option() -> Callable:
    return click.option(
        "--stages",
        type=str,
        default="fetch,parse,normalize",
        callback=lambda ctx, param, value: [
            common.PipelineStage(item.strip().lower()) for item in value.split(",")
        ],
    )


def _enrich_apis_option() -> Callable:
    return click.option(
        "--enrich-apis",
        "enrich_apis",
        type=str,
        default=lambda: os.environ.get("ENRICH_APIS", ""),
        callback=lambda ctx, param, value: set(
            [item.strip().lower() for item in value.split(",")]
        ),
    )


def _geocodio_apikey_option() -> Callable:
    return click.option(
        "--geocodio-apikey",
        "geocodio_apikey",
        type=str,
        default=lambda: os.environ.get("GEOCODIO_APIKEY", ""),
    )


def _placekey_apikey_option() -> Callable:
    return click.option(
        "--placekey-apikey",
        "placekey_apikey",
        type=str,
        default=lambda: os.environ.get("PLACEKEY_APIKEY", ""),
    )


def _fail_on_error_option() -> Callable:
    return click.option(
        "--fail-on-runner-error/--no-fail-on-runner-error",
        type=bool,
        default=True,
        help="When set (default), errors in runners will raise",
    )


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


def _rematch_option() -> Callable:
    return click.option(
        "--rematch/--no-rematch",
        "enable_rematch",
        type=bool,
        default=lambda: os.environ.get("ENABLE_REMATCH", "false").lower() == "true",
    )


def _reimport_option() -> Callable:
    return click.option(
        "--reimport/--no-reimport",
        "enable_reimport",
        type=bool,
        default=lambda: os.environ.get("ENABLE_REIMPORT", "false").lower() == "true",
    )


def _match_ids_option() -> Callable:
    return click.option(
        "--match-ids",
        "match_ids",
        type=str,
        callback=lambda ctx, param, value: (
            {
                key: value
                for key, value in [
                    [v.strip() for v in pair.split("=", maxsplit=1)]
                    for pair in [item.strip() for item in value.split(",") if item]
                    if pair and "=" in pair
                ]
            }
            if value
            else None
        ),
    )


def _api_cache_option() -> Callable:
    return click.option(
        "--api-cache/--no-api-cache",
        "enable_apicache",
        type=bool,
        default=lambda: os.environ.get("ENABLE_APICACHE", "true").lower() == "true",
    )


def _create_ids_option() -> Callable:
    return click.option(
        "--create-ids",
        "create_ids",
        type=str,
        callback=lambda ctx, param, value: (
            [item.strip() for item in value.split(",")] if value else None
        ),
    )


def _candidate_distance_option() -> Callable:
    return click.option(
        "--candidate-distance",
        "candidate_distance",
        type=float,
        default=CANDIDATE_DEGREES_DISTANCE,
    )


def _import_batch_size_option() -> Callable:
    return click.option(
        "--import-batch-size",
        "import_batch_size",
        type=int,
        default=lambda: os.environ.get("IMPORT_BATCH_SIZE", IMPORT_BATCH_SIZE),
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
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_fail_on_error_option()
def fetch(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    fail_on_runner_error: bool,
) -> None:
    """Run fetch process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    for site_dir in site_dirs:
        ingest.run_fetch(
            site_dir,
            output_dir,
            timestamp,
            dry_run,
            fail_on_runner_error=fail_on_runner_error,
        )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_validate_option()
@_fail_on_error_option()
def parse(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    validate: bool,
    fail_on_runner_error: bool,
) -> None:
    """Run parse process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    for site_dir in site_dirs:
        ingest.run_parse(
            site_dir,
            output_dir,
            timestamp,
            validate,
            dry_run,
            fail_on_runner_error=fail_on_runner_error,
        )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_validate_option()
@_fail_on_error_option()
def normalize(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    validate: bool,
    fail_on_runner_error: bool,
) -> None:
    """Run normalize process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    for site_dir in site_dirs:
        ingest.run_normalize(
            site_dir,
            output_dir,
            timestamp,
            validate,
            dry_run,
            fail_on_runner_error=fail_on_runner_error,
        )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@_fail_on_error_option()
def all_stages(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    fail_on_runner_error: bool,
) -> None:
    """Run all stages in succession for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    for site_dir in site_dirs:
        fetch_success = ingest.run_fetch(
            site_dir, output_dir, timestamp, fail_on_runner_error=fail_on_runner_error
        )

        if not fetch_success:
            continue

        parse_success = ingest.run_parse(
            site_dir, output_dir, timestamp, fail_on_runner_error=fail_on_runner_error
        )

        if not parse_success:
            continue

        ingest.run_normalize(
            site_dir, output_dir, timestamp, fail_on_runner_error=fail_on_runner_error
        )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@_api_cache_option()
@_enrich_apis_option()
@_geocodio_apikey_option()
@_placekey_apikey_option()
@_dry_run_option()
def enrich(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    enable_apicache: bool,
    enrich_apis: Optional[Collection[str]],
    geocodio_apikey: Optional[str],
    placekey_apikey: Optional[str],
    dry_run: bool,
) -> None:
    """Run enrich process for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    for site_dir in site_dirs:
        ingest.run_enrich(
            site_dir,
            output_dir,
            timestamp,
            enable_apicache=enable_apicache,
            enrich_apis=enrich_apis,
            geocodio_apikey=geocodio_apikey,
            placekey_apikey=placekey_apikey,
            dry_run=dry_run,
        )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_vial_server_option()
@_vial_apikey_option()
@_match_option()
@_create_option()
@_rematch_option()
@_reimport_option()
@_match_ids_option()
@_create_ids_option()
@_candidate_distance_option()
@_import_batch_size_option()
def load_to_vial(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    vial_server: str,
    vial_apikey: str,
    enable_match: bool,
    enable_create: bool,
    enable_rematch: bool,
    enable_reimport: bool,
    match_ids: Optional[Dict[str, str]],
    create_ids: Optional[Collection[str]],
    candidate_distance: float,
    import_batch_size: int,
) -> None:
    """Load specified sites to vial server."""
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    if match_ids and create_ids:
        conflicting_ids = set(match_ids.keys()) & set(create_ids)
        if conflicting_ids:
            raise Exception(
                f"Conflicting match and create action for source ids: {conflicting_ids}"
            )

    load.load_sites_to_vial(
        site_dirs,
        output_dir,
        dry_run=dry_run,
        vial_server=vial_server,
        vial_apikey=vial_apikey,
        enable_match=enable_match,
        enable_create=enable_create,
        enable_rematch=enable_rematch,
        enable_reimport=enable_reimport,
        match_ids=match_ids,
        create_ids=create_ids,
        candidate_distance=candidate_distance,
        import_batch_size=import_batch_size,
    )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@_dry_run_option()
@_stages_option()
@_api_cache_option()
@_enrich_apis_option()
@_geocodio_apikey_option()
@_placekey_apikey_option()
@_vial_server_option()
@_vial_apikey_option()
@_match_option()
@_create_option()
@_rematch_option()
@_reimport_option()
@_match_ids_option()
@_create_ids_option()
@_candidate_distance_option()
@_import_batch_size_option()
@_fail_on_error_option()
def pipeline(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    dry_run: bool,
    stages: Collection[common.PipelineStage],
    enable_apicache: bool,
    enrich_apis: Optional[Collection[str]],
    geocodio_apikey: Optional[str],
    placekey_apikey: Optional[str],
    vial_server: Optional[str],
    vial_apikey: Optional[str],
    enable_match: bool,
    enable_create: bool,
    enable_rematch: bool,
    enable_reimport: bool,
    match_ids: Optional[Dict[str, str]],
    create_ids: Optional[Collection[str]],
    candidate_distance: float,
    import_batch_size: int,
    fail_on_runner_error: bool,
) -> None:
    """Run all stages in succession for specified sites."""
    timestamp = _generate_run_timestamp()
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    sites_to_load = []

    for site_dir in site_dirs:
        if common.PipelineStage.FETCH in stages:
            fetch_success = ingest.run_fetch(
                site_dir,
                output_dir,
                timestamp,
                fail_on_runner_error=fail_on_runner_error,
            )

            if not fetch_success:
                continue

        if common.PipelineStage.PARSE in stages:
            parse_success = ingest.run_parse(
                site_dir,
                output_dir,
                timestamp,
                fail_on_runner_error=fail_on_runner_error,
            )

            if not parse_success:
                continue

        if common.PipelineStage.NORMALIZE in stages:
            normalize_success = ingest.run_normalize(
                site_dir,
                output_dir,
                timestamp,
                fail_on_runner_error=fail_on_runner_error,
            )

            if not normalize_success:
                continue

        if common.PipelineStage.ENRICH in stages:
            enrich_success = ingest.run_enrich(
                site_dir,
                output_dir,
                timestamp,
                enable_apicache=enable_apicache,
                enrich_apis=enrich_apis,
                geocodio_apikey=geocodio_apikey,
                placekey_apikey=placekey_apikey,
            )

            if not enrich_success:
                continue

        sites_to_load.append(site_dir)

    if common.PipelineStage.LOAD_TO_VIAL in stages and sites_to_load:
        if not vial_server:
            raise Exception("Must pass --vial-server for load-to-vial stage")

        if not vial_apikey:
            raise Exception("Must pass --vial-apikey for load-to-vial stage")

        load.load_sites_to_vial(
            sites_to_load,
            output_dir,
            dry_run=dry_run,
            vial_server=vial_server,
            vial_apikey=vial_apikey,
            enable_match=enable_match,
            enable_create=enable_create,
            enable_rematch=enable_rematch,
            enable_reimport=enable_reimport,
            match_ids=match_ids,
            create_ids=create_ids,
            candidate_distance=candidate_distance,
            import_batch_size=import_batch_size,
        )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
def api_cache_remove(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
) -> None:
    """Remove the api cache for specified sites."""
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    for site_dir in site_dirs:
        caching.remove_api_cache(
            output_dir,
            site_dir,
            common.PipelineStage.ENRICH,
        )


@cli.command()
@_sites_argument()
@_exclude_sites_option()
@_state_option()
@_output_dir_option()
@click.option("--cache-tag", "cache_tag", type=str)
def api_cache_evict(
    sites: Optional[Sequence[str]],
    exclude_sites: Optional[Collection[str]],
    state: Optional[str],
    output_dir: pathlib.Path,
    cache_tag: str,
) -> None:
    """Evict keys with tag from the api cache for specified sites."""
    site_dirs = site.get_site_dirs(state, sites, exclude_sites)

    for site_dir in site_dirs:
        num_evicted_keys = caching.evict_api_cache(
            output_dir,
            site_dir,
            common.PipelineStage.ENRICH,
            cache_tag,
        )
        if num_evicted_keys > 0:
            click.echo(f"Evicted {num_evicted_keys} keys from {site_dir}")


@cli.command()
def version() -> None:
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    cli()
