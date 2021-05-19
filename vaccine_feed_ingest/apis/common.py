"""Common API tooling"""
import hashlib
import random
from typing import Any, Optional, Sequence

import diskcache

DEFAULT_EXPIRE_SECONDS = 45 * 24 * 60 * 60  # 40 days
DEFAULT_EXPIRE_JIGGLE_PERCENT = 0.10  # +/- 4 days


def calculate_cache_key(name: str, args: Sequence[str]) -> str:
    """Convert a cache key from a sequence of str values"""
    hasher = hashlib.md5()

    for value in args:
        hasher.update(value.encode())

    args_hash = hasher.hexdigest()

    return f"{name}:{args_hash}"


class CachedAPI:
    """API for calling placekey that checks the cache first"""

    def __init__(
        self,
        api_cache: diskcache.Cache,
        expire_secs: Optional[float] = None,
        expire_jiggle_percent: Optional[float] = None,
    ):
        self.api_cache = api_cache

        if expire_secs is None:
            expire_secs = DEFAULT_EXPIRE_SECONDS

        if expire_jiggle_percent is None:
            expire_jiggle_percent = DEFAULT_EXPIRE_JIGGLE_PERCENT

        self._expire_min_secs = expire_secs - expire_secs * expire_jiggle_percent
        self._expire_max_secs = expire_secs + expire_secs * expire_jiggle_percent

    def _calculate_expire(self) -> float:
        """Calculate a random expire number between the range.

        This is so we don't expire all of the locations at the same time.
        """
        return random.uniform(self._expire_min_secs, self._expire_max_secs)

    def set_with_expire(self, key: str, value: Any) -> bool:
        return self.api_cache.set(key, value, expire=self._calculate_expire())
