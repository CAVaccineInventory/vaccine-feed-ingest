"""Load and store a diskcache from remote storage"""

import contextlib
import pathlib
import shutil
import tarfile
import tempfile
from typing import Iterator

import diskcache

from . import common, outputs


@contextlib.contextmanager
def api_cache_for_stage(
    base_output_dir: pathlib.Path,
    site_dir: pathlib.Path,
    stage: common.PipelineStage,
) -> Iterator[diskcache.Cache]:
    """Load api cache from archive for the specified site and stage"""
    api_cache_path = outputs.generate_api_cache_path(
        base_output_dir,
        site_dir.parent.name,
        site_dir.name,
        stage,
    )

    with cache_from_archive(api_cache_path) as api_cache:
        yield api_cache


def remove_api_cache(
    base_output_dir: pathlib.Path,
    site_dir: pathlib.Path,
    stage: common.PipelineStage,
) -> None:
    api_cache_path = outputs.generate_api_cache_path(
        base_output_dir,
        site_dir.parent.name,
        site_dir.name,
        stage,
    )

    api_cache_path.unlink(missing_ok=True)


def evict_api_cache(
    base_output_dir: pathlib.Path,
    site_dir: pathlib.Path,
    stage: common.PipelineStage,
    tag: str,
) -> int:
    api_cache_path = outputs.generate_api_cache_path(
        base_output_dir,
        site_dir.parent.name,
        site_dir.name,
        stage,
    )

    if not api_cache_path.exists():
        return 0

    with api_cache_for_stage(base_output_dir, site_dir, stage) as api_cache:
        return api_cache.evict(tag)


@contextlib.contextmanager
def cache_from_archive(archive_path: pathlib.Path) -> Iterator[diskcache.Cache]:
    """Load a diskcache from remote archive, and write it back when done"""

    with tempfile.TemporaryDirectory("_cache") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        tmp_diskcache_dir = tmp_dir / "diskcache"
        tmp_diskcache_dir.mkdir()

        # If there is an existing archive file, then extract the diskcache from it.
        if archive_path.exists():
            with archive_path.open(mode="rb") as archive_file:
                with tarfile.open(fileobj=archive_file, mode="r|gz") as tar_archive:
                    tar_archive.extractall(tmp_diskcache_dir)

        with diskcache.Cache(
            tmp_diskcache_dir,
            disk=diskcache.JSONDisk,
            disk_compress_level=1,
            eviction_policy="least-frequently-used",
        ) as cache:
            # Before using cache cull expired items
            cache.cull()

            yield cache

            # After done with cache, cull expired items
            cache.cull()

        tmp_archive_name = tmp_dir / "diskcache"
        tmp_archive_str = shutil.make_archive(
            str(tmp_archive_name), "gztar", tmp_diskcache_dir
        )

        tmp_archive_path = pathlib.Path(tmp_archive_str)

        archive_path.parent.mkdir(parents=True, exist_ok=True)

        # Overwrite cache archive with new data
        with tmp_archive_path.open("rb") as src_file:
            with archive_path.open("wb") as dst_file:
                for content in src_file:
                    dst_file.write(content)
