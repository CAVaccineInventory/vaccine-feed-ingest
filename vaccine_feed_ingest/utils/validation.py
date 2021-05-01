"""Shared constants for validation"""

from pydantic import BaseModel
from vaccine_feed_ingest_schema import location


class MinMax(BaseModel):
    minimum: float
    maximum: float

    def contains(self, x: float) -> bool:
        return x > self.minimum and x < self.maximum


class BoundingBox(BaseModel):
    latitude: MinMax
    longitude: MinMax

    def contains(self, lat_lng: location.LatLng) -> bool:
        return self.latitude.contains(lat_lng.latitude) and self.longitude.contains(
            lat_lng.longitude
        )


BOUNDING_BOX = BoundingBox(
    latitude=MinMax(
        minimum=-14.549,
        maximum=71.367,
    ),
    longitude=MinMax(
        minimum=-179.779,
        maximum=0.0,
    ),
)

BOUNDING_BOX_GUAM = BoundingBox(
    latitude=MinMax(
        minimum=12.613640,
        maximum=16.816439,
    ),
    longitude=MinMax(
        minimum=144.064088,
        maximum=146.330609,
    ),
)
