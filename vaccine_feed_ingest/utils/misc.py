"""Miscellaneous python utils"""
import itertools
from typing import Iterable, Iterator


def batch(iterable: Iterable, size: int) -> Iterator[Iterator]:
    """Batch an iterable into chunks of specified size"""
    iterator = iter(iterable)

    while True:
        batch_iterator = list(itertools.islice(iterator, size))

        if not batch_iterator:
            return

        yield iter(batch_iterator)
