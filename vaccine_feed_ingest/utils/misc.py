"""Miscellaneous python utils"""
import itertools
from typing import Iterable, Iterator


def batch(iterable: Iterable, size: int) -> Iterator[Iterator]:
    """Batch an interable into chunks of specified size"""
    iterator = iter(iterable)
    while True:
        batch_iterator = itertools.islice(iterator, size)
        try:
            yield itertools.chain([next(batch_iterator)], batch_iterator)
        except StopIteration:
            # As of PEP 479 (released in python 3.7), cannot raise StopIteration
            # from inside a generator.
            return
