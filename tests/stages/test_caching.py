import pathlib

from vaccine_feed_ingest.stages import caching


def test_cache_from_archive(tmpdir):
    tmp_path = pathlib.Path(tmpdir)
    archive_path = tmp_path / "archive.tar.gz"

    assert not archive_path.exists()

    with caching.cache_from_archive(archive_path) as cache:
        assert cache is not None

        assert not cache.get("key")
        cache.set("key", "value")
        assert cache.get("key") == "value"

    assert archive_path.exists()

    with caching.cache_from_archive(archive_path) as cache:
        assert cache is not None

        assert cache.get("key")
