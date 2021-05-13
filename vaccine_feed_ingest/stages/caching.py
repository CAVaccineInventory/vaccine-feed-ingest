"""Load and store a diskcache from remote storage"""

import contextlib
import tempfile
import pathlib
import tarfile
import shutil
from typing import Iterator

import diskcache


@contextlib.contextmanager
def cache_from_archive(archive_path: pathlib.Path) -> Iterator[diskcache.Cache]:
    """Load a diskcache from remote archive, and write it back when done"""

    with tempfile.TemporaryDirectory("_cache") as tmp_str:
        tmp_dir = pathlib.Path(tmp_str)

        tmp_diskcache_dir = tmp_dir / "diskcache"
        tmp_diskcache_dir.mkdir()

        # If there is an existing archive file, then extract the diskcache from it.
        if archive_path.exists():
            with tarfile.open(archive_path, mode="r|gz") as archive_file:
                archive_file.extractall(tmp_diskcache_dir)

        with diskcache.Cache(
            tmp_diskcache_dir, disk=diskcache.JSONDisk, disk_compress_level=1
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

        # Overwrite cache archive with new data
        with tmp_archive_path.open("rb") as src_file:
            with archive_path.open("wb") as dst_file:
                for content in src_file:
                    dst_file.write(content)
