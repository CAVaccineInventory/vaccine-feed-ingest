"""Miscellaneous python utils"""
import itertools
from typing import Any, Dict, Iterable, Iterator


def batch(iterable: Iterable, size: int) -> Iterator[Iterator]:
    """Batch an iterable into chunks of specified size"""
    iterator = iter(iterable)

    while True:
        batch_iterator = list(itertools.islice(iterator, size))

        if not batch_iterator:
            return

        yield iter(batch_iterator)


def dict_batch(dictionary: Dict[Any, Any], size: int) -> Iterator[Dict[Any, Any]]:
    iterator = iter(dictionary)

    while True:
        batch_iterator = list(itertools.islice(iterator, size))

        if not batch_iterator:
            return

        batch = {k: dictionary[k] for k in batch_iterator}

        yield batch
