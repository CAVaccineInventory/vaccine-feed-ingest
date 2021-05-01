from typing import Iterator

from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.stages import ingest
from vaccine_feed_ingest.utils.validation import BoundingBox, MinMax


def test_validate_bounding_boxes():
    bb1 = BoundingBox(
        latitude=MinMax(
            minimum=-10.0,
            maximum=0.0,
        ),
        longitude=MinMax(
            minimum=-20.1,
            maximum=20.1,
        ),
    )
    bb2 = BoundingBox(
        latitude=MinMax(
            minimum=-0.0,
            maximum=10.0,
        ),
        longitude=MinMax(
            minimum=-20.1,
            maximum=20.1,
        ),
    )

    assert ingest.validate_bounding_boxes(
        location.LatLng(latitude=5, longitude=5), [bb1, bb2]
    )
    assert ingest.validate_bounding_boxes(
        location.LatLng(latitude=-5, longitude=5), [bb1, bb2]
    )
    assert not ingest.validate_bounding_boxes(
        location.LatLng(latitude=50, longitude=50), [bb1, bb2]
    )

    # Bounding box boundaries are exclusive
    assert not ingest.validate_bounding_boxes(
        location.LatLng(latitude=0, longitude=0), [bb1, bb2]
    )
