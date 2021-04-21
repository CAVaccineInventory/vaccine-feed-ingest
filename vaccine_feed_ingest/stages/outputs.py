"""Helper methods for managing data for each stage"""

import pathlib
from typing import Iterator, Optional

from .common import STAGE_OUTPUT_NAME, PipelineStage


def find_all_run_dirs(
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


def find_latest_run_dir(
    base_output_dir: pathlib.Path,
    state: str,
    site: str,
    stage: PipelineStage,
) -> Optional[pathlib.Path]:
    """Find latest stage output path"""
    return next(find_all_run_dirs(base_output_dir, state, site, stage), None)


def generate_run_dir(
    base_output_dir: pathlib.Path,
    state: str,
    site: str,
    stage: PipelineStage,
    timestamp: str,
) -> pathlib.Path:
    """Generate output path for a pipeline stage."""
    return base_output_dir / state / site / STAGE_OUTPUT_NAME[stage] / timestamp


def iter_data_paths(data_dir: pathlib.Path) -> Iterator[pathlib.Path]:
    """Return paths to data files in data_dir.

    Directories and files that start with `_` or `.` are ignored.
    """
    for filepath in data_dir.iterdir():
        if filepath.name.startswith("_") or filepath.name.startswith("."):
            continue

        yield filepath


def data_exists(data_dir: pathlib.Path) -> bool:
    """Returns true if there are data files in data_dir.

    Directories and files that start with `_` or `.` are ignored."""
    return bool(next(iter_data_paths(data_dir), None))


def copy_files(src_dir: pathlib.Path, dst_dir: pathlib.Path) -> None:
    """Copy all files in src_dir to dst_dir.

    Directories and files that start with `_` or `.` are ignored.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)

    for filepath in iter_data_paths(src_dir):
        with filepath.open("rb") as src_file:
            with (dst_dir / filepath.name).open("wb") as dst_file:
                for content in src_file:
                    dst_file.write(content)
