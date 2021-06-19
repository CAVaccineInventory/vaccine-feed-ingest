"""Miscellaneous python utils"""
import itertools
from typing import Any, Dict, Iterable, Iterator, Tuple, TypeVar


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


T = TypeVar("T")


def peek_iter(elements: Iterator[T]) -> Tuple[T, Iterator[T]]:
    """Peek at first element of iterator.
    Returns first element and iterator of elements (including first element).
    Or, raises StopIteration if there are no elements.
    """
    first_element = next(elements)
    return first_element, itertools.chain([first_element], elements)


def at_least_iter(elements: Iterator[T], size: int) -> Tuple[bool, Iterator[T]]:
    """Returns if iterator is at least as long as the specified size."""
    # Peek if tiles iterator is over the threashold and reassemble the iterator.
    first_elements = list(itertools.islice(elements, size + 1))
    return len(first_elements) > size, itertools.chain(first_elements, elements)


def exists_iter(elements: Iterator[T]) -> Tuple[bool, Iterator[T]]:
    """Returns if iterator contains at one element"""
    return at_least_iter(elements, 1)
