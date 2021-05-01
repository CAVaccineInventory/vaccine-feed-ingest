from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils import validation


def test_bounding_box_contains():
    box = validation.BoundingBox(
        latitude=validation.MinMax(
            minimum=-10.0,
            maximum=0.0,
        ),
        longitude=validation.MinMax(
            minimum=-20.0,
            maximum=20.0,
        ),
    )
    assert box.contains(location.LatLng(latitude=-5, longitude=10))
    assert not box.contains(location.LatLng(latitude=10, longitude=10))
    assert not box.contains(location.LatLng(latitude=-5, longitude=100))


def test_bounding_boxes_contains():
    boxes = validation.BoundingBoxes(
        boxes=[
            validation.BoundingBox(
                latitude=validation.MinMax(
                    minimum=-10.0,
                    maximum=0.0,
                ),
                longitude=validation.MinMax(
                    minimum=-20.1,
                    maximum=20.1,
                ),
            ),
            validation.BoundingBox(
                latitude=validation.MinMax(
                    minimum=-0.0,
                    maximum=10.0,
                ),
                longitude=validation.MinMax(
                    minimum=-20.1,
                    maximum=20.1,
                ),
            ),
        ]
    )

    assert boxes.contains(location.LatLng(latitude=5, longitude=5))
    assert boxes.contains(location.LatLng(latitude=-5, longitude=5))
    assert not boxes.contains(location.LatLng(latitude=50, longitude=50))

    # Bounding box boundaries are exclusive
    assert not boxes.contains(location.LatLng(latitude=0, longitude=0))
