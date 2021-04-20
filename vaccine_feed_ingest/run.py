#!/usr/bin/env python

"""
Entry point for running vaccine feed runners
"""
import datetime
import enum
import json
import logging
import os
import pathlib
import subprocess
import tempfile
from typing import Iterator, Optional, Sequence

import click
import dotenv
import pathy
import pydantic
import urllib3
from vaccine_feed_ingest.schema import schema

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
    if sites:
        for site in sites:
            site_dir = _get_site_dir(site)
            if not site_dir:
                continue

            yield site_dir

    else:
        yield from _get_site_dirs_for_state(state)


def _find_relevant_file(
    site_dir: pathlib.Path,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find file. Logs an error and returns false if something is wrong."""
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

    return cmds[0]


def _find_executeable(
    site_dir: pathlib.Path,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find executable. Logs and returns false if something is wrong."""
    cmd = _find_relevant_file(site_dir, stage)

    if not cmd:
        return None

    if not os.access(cmd, os.X_OK):
        logger.warn("%s in %s is not marked as executable.", cmd.name, str(site_dir))
        return None

    return cmd


def _find_yml(
    site_dir: pathlib.Path,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find yml file. Logs and returns false if something is wrong."""
    yml = _find_relevant_file(site_dir, stage)

    if not yml:
        return None

    _, extension = os.path.splitext(yml)

    if not extension == ".yml":
        logger.warn("%s in %s is not a .yml file", yml.name, str(site_dir))
        return None

    if not os.access(yml, os.R_OK):
        logger.warn("%s in %s is not marked as readable.", yml.name, str(site_dir))
        return None

    return yml


def _generate_run_timestamp() -> str:
    return datetime.datetime.now().replace(microsecond=0).isoformat()


def _find_all_run_dirs(
    base_output_dir: pathlib.Path,
    state: str,
    site: str,
    stage: PipelineStage,
) -> Iterator[pathlib.Path]:
    """Find latest stage output path"""
    stage_dir = base_output_dir / state / site / STAGE_OUTPUT_NAME[stage]

    if not stage_dir.exists():
        return

    for run_dir in sorted(stage_dir.iterdir(), reverse=True):
        if run_dir.name.startswith("_"):
            continue

        if run_dir.name.startswith("."):
            continue

        yield run_dir


def _find_latest_run_dir(
    base_output_dir: pathlib.Path,
    state: str,
    site: str,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find latest stage output path"""
    return next(_find_all_run_dirs(base_output_dir, state, site, stage), None)


def _generate_run_dir(
    base_output_dir: pathlib.Path,
    state: str,
    site: str,
    stage: PipelineStage,
    timestamp: str,
) -> pathlib.Path:
    """Generate output path for a pipeline stage."""
    return base_output_dir / state / site / STAGE_OUTPUT_NAME[stage] / timestamp


def _iter_data_paths(data_dir: pathlib.Path) -> Iterator[pathlib.Path]:
    """Return paths to data files in data_dir.

    Directories and files that start with `_` or `.` are ignored.
    """
    for filepath in data_dir.iterdir():
        if filepath.name.startswith("_") or filepath.name.startswith("."):
            continue

        yield filepath


def _data_exists(data_dir: pathlib.Path) -> bool:
    """Returns true if there are data files in data_dir.

    Directories and files that start with `_` or `.` are ignored."""
    return bool(next(_iter_data_paths(data_dir), None))


def _copy_files(src_dir: pathlib.Path, dst_dir: pathlib.Path) -> None:
    """Copy all files in src_dir to dst_dir.

    Directories and files that start with `_` or `.` are ignored.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)

    for filepath in _iter_data_paths(src_dir):
        with filepath.open("rb") as src_file:
            with (dst_dir / filepath.name).open("wb") as dst_file:
                for content in src_file:
                    dst_file.write(content)


def _run_fetch(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
) -> bool:
    fetch_path = _find_executeable(site_dir, PipelineStage.FETCH)

    yml_path = None
    if not fetch_path:
        yml_path = _find_yml(site_dir, PipelineStage.FETCH)

        if not yml_path:
            logger.info("No fetch cmd or .yml config for %s to run.", site_dir.name)
            return False

        fetch_path = _find_executeable(
            RUNNERS_DIR.joinpath("_shared"), PipelineStage.FETCH
        )

    fetch_run_dir = _generate_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.FETCH, timestamp
    )

    with tempfile.TemporaryDirectory(
        f"_fetch_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)
        fetch_output_dir = tmp_dir / "output"
        fetch_output_dir.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            [str(fetch_path), str(fetch_output_dir), str(yml_path)], check=True
        )

        if not _data_exists(fetch_output_dir):
            logger.warning(
                "%s for %s returned no data files.", fetch_path.name, site_dir.name
            )
            return False

        _copy_files(fetch_output_dir, fetch_run_dir)

    return True


def _run_parse(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
) -> bool:
    parse_path = _find_executeable(site_dir, PipelineStage.PARSE)
    yml_path = None
    if not parse_path:
        yml_path = _find_yml(site_dir, PipelineStage.PARSE)

        if not yml_path:
            logger.info("No parse cmd or .yml config for %s to run.", site_dir.name)
            return False

        parse_path = _find_executeable(
            RUNNERS_DIR.joinpath("_shared"), PipelineStage.PARSE
        )

    fetch_run_dir = _find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.FETCH
    )
    if not fetch_run_dir:
        logger.warning(
            "Skipping parse stage for %s because there is no data from fetch stage",
            site_dir.name,
        )
        return False

    if not _data_exists(fetch_run_dir):
        logger.warning("No fetch data available to parse for %s.", site_dir.name)
        return False

    parse_run_dir = _generate_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.PARSE, timestamp
    )

    with tempfile.TemporaryDirectory(
        f"_parse_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        parse_output_dir = tmp_dir / "output"
        parse_input_dir = tmp_dir / "input"

        parse_output_dir.mkdir(parents=True, exist_ok=True)
        parse_input_dir.mkdir(parents=True, exist_ok=True)

        _copy_files(fetch_run_dir, parse_input_dir)

        subprocess.run(
            [
                str(parse_path),
                str(parse_output_dir),
                str(parse_input_dir),
                str(yml_path),
            ],
            check=True,
        )

        if not _data_exists(parse_output_dir):
            logger.warning(
                "%s for %s returned no data files.", parse_path.name, site_dir.name
            )
            return False

        _copy_files(parse_output_dir, parse_run_dir)

    return True


def _run_normalize(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    timestamp: str,
) -> bool:
    normalize_path = _find_executeable(site_dir, PipelineStage.NORMALIZE)
    if not normalize_path:
        logger.info("No normalize cmd for %s to run.", site_dir.name)
        return False

    parse_run_dir = _find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.PARSE
    )
    if not parse_run_dir:
        logger.warning(
            "Skipping normalize stage for %s because there is no data from parse stage",
            site_dir.name,
        )
        return False

    if not _data_exists(parse_run_dir):
        logger.warning("No parse data available to normalize for %s.", site_dir.name)
        return False

    normalize_run_dir = _generate_run_dir(
        output_dir,
        site_dir.parent.name,
        site_dir.name,
        PipelineStage.NORMALIZE,
        timestamp,
    )

    with tempfile.TemporaryDirectory(
        f"_normalize_{site_dir.parent.name}_{site_dir.name}"
    ) as tmp_str:
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

        if not _data_exists(normalize_output_dir):
            logger.warning(
                "%s for %s returned no data files.", normalize_path.name, site_dir.name
            )
            return False

        _copy_files(normalize_output_dir, normalize_run_dir)

    return True


def _run_load_to_vial(
    vial_http: urllib3.connectionpool.ConnectionPool,
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    import_run_id: str,
) -> bool:
    normalize_run_dir = _find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.NORMALIZE
    )
    if not normalize_run_dir:
        logger.warning(
            "Skipping load for %s because there is no data from normalize stage",
            site_dir.name,
        )
        return False

    if not _data_exists(normalize_run_dir):
        logger.warning("No normalize data available to load for %s.", site_dir.name)
        return False

    num_imported_locations = 0

    for filepath in _iter_data_paths(normalize_run_dir):
        if not filepath.name.endswith(".normalized.ndjson"):
            continue

        import_locations = []
        with filepath.open("rb") as src_file:
            for line in src_file:
                try:
                    normalized_location = schema.NormalizedLocation.parse_raw(line)
                except pydantic.ValidationError:
                    logger.warning(
                        "Skipping source location because it is invalid: %s",
                        line,
                        exc_info=True,
                    )
                    continue

                import_locations.append(_create_import_location(normalized_location))

        if not import_locations:
            logger.warning(
                "No locations to import in %s in %s",
                filepath.name,
                site_dir.name,
            )
            continue

        encoded_ndjson = "\n".join([loc.json() for loc in import_locations])

        import_resp = vial_http.request(
            "POST",
            f"/api/importSourceLocations?import_run_id={import_run_id}",
            headers={**vial_http.headers, "Content-Type": "application/x-ndjson"},
            body=encoded_ndjson.encode("utf-8"),
        )

        if import_resp.status != 200:
            logger.warning(
                "Failed to import source locations for %s in %s: %s",
                filepath.name,
                site_dir.name,
                import_resp.data[:100],
            )

        num_imported_locations += len(import_locations)

    logger.info(
        "Imported %d source locations for %s",
        num_imported_locations,
        site_dir.name,
    )

    return bool(num_imported_locations)


def _create_import_location(
    normalized_record: schema.NormalizedLocation,
) -> schema.ImportSourceLocation:
    """Transform normalized record into import record"""
    import_location = schema.ImportSourceLocation(
        source_uid=normalized_record.id,
        source_name=normalized_record.source.source,
        import_json=normalized_record,
        # TODO: Add code to match to existing entities
        match=schema.ImportMatchAction(action="new"),
    )

    if normalized_record.name:
        import_location.name = normalized_record.name

    if normalized_record.location:
        import_location.latitude = normalized_record.location.latitude
        import_location.longitude = normalized_record.location.longitude

    return import_location


@click.group()
def cli():
    """Run vaccine-feed-ingest commands"""
    dotenv.load_dotenv()


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
    site_dirs = list(_get_site_dirs(state, sites))

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
        fetch_success = _run_fetch(site_dir, output_dir, timestamp)

        if not fetch_success:
            continue

        parse_success = _run_parse(site_dir, output_dir, timestamp)

        if not parse_success:
            continue

        _run_normalize(site_dir, output_dir, timestamp)


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
@click.option("--state", "state", type=str)
@click.option(
    "--output-dir", "output_dir", type=str, default="out", callback=_pathy_data_path
)
@click.argument("sites", nargs=-1, type=str)
def load_to_vial(
    vial_server: str,
    vial_apikey: str,
    state: Optional[str],
    output_dir: pathlib.Path,
    sites: Optional[Sequence[str]],
) -> None:
    """Load specified sites to vial server."""
    if not vial_server:
        raise Exception("Must configure VIAL server to call")

    if not vial_apikey:
        raise Exception("Must configure VIAL API Key to use")

    site_dirs = _get_site_dirs(state, sites)

    http_pool = urllib3.PoolManager()
    vial_http = http_pool.connection_from_url(
        vial_server,
        pool_kwargs={"headers": {"Authorization": f"Bearer {vial_apikey}"}},
    )

    verify_resp = vial_http.request("GET", "/api/verifyToken")
    if verify_resp.status != 200:
        raise Exception(f"Invalid api key for VIAL server: {verify_resp.data}")

    import_resp = vial_http.request("POST", "/api/startImportRun")
    if import_resp.status != 200:
        raise Exception(f"Failed to start import run {import_resp.data}")

    import_data = json.loads(import_resp.data.decode("utf-8"))
    import_run_id = import_data.get("import_run_id")

    if not import_run_id:
        raise Exception(f"Failed to start import run {import_data}")

    for site_dir in site_dirs:
        _run_load_to_vial(
            vial_http,
            site_dir,
            output_dir,
            import_run_id,
        )


@cli.command()
def version() -> None:
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    cli()
