"""Helper methods for finding code and configs for each site"""

import logging
import os
import pathlib
from typing import Iterator, Optional, Sequence

from .common import RUNNERS_DIR, STAGE_CMD_NAME, PipelineStage

logger = logging.getLogger("ingest")


def get_site_dirs_for_state(state: Optional[str] = None) -> Iterator[pathlib.Path]:
    """Return an iterator of site directory paths"""
    for state_dir in RUNNERS_DIR.iterdir():
        # Ignore private directories, in this case the _template directory
        if state_dir.name.startswith("_"):
            continue

        if state and state_dir.name.lower() != state.lower():
            continue

        yield from state_dir.iterdir()


def get_site_dir(site: str) -> Optional[pathlib.Path]:
    """Return a site directory path, if it exists"""
    site_dir = RUNNERS_DIR / site

    if not site_dir.exists():
        return None

    return site_dir


def get_site_dirs(
    state: Optional[str], sites: Optional[Sequence[str]]
) -> Iterator[pathlib.Path]:
    """Return a site directory path, if it exists"""
    if sites:
        for site in sites:
            site_dir = get_site_dir(site)
            if not site_dir:
                continue

            yield site_dir

    else:
        yield from get_site_dirs_for_state(state)


def find_relevant_file(
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


def find_executeable(
    site_dir: pathlib.Path,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find executable. Logs and returns false if something is wrong."""
    cmd = find_relevant_file(site_dir, stage)

    if not cmd:
        return None

    # yml files do not need to be executable
    if cmd.name.endswith(".yml"):
        return None

    if not os.access(cmd, os.X_OK):
        logger.warn("%s in %s is not marked as executable.", cmd.name, str(site_dir))
        return None

    return cmd


def find_yml(
    site_dir: pathlib.Path,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find yml file. Logs and returns false if something is wrong."""
    yml = find_relevant_file(site_dir, stage)

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
