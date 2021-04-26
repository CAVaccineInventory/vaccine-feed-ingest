from typing import Iterator

from vaccine_feed_ingest.utils import misc


def test_batch():
    orig_items = [1, 2, 3, 4, 5]

    batched_iter = misc.batch(orig_items, 3)

    assert batched_iter is not None
    assert isinstance(batched_iter, Iterator)

    batched_list = list(batched_iter)
    assert len(batched_list) == 2

    final_items = []
    for group_iter in batched_list:
        assert group_iter is not None
        assert isinstance(group_iter, Iterator)

        group_list = list(group_iter)
        print(group_list)
        assert len(group_list) <= 3

        final_items.extend(group_list)

    assert orig_items == final_items
