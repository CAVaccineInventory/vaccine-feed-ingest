"""Shared constants for validation"""

from pydantic import BaseModel


class MinMax(BaseModel):
    minimum: float
    maximum: float

    def contains(self, x: float) -> bool:
        return x > self.minimum and x < self.maximum


class BoundingBox(BaseModel):
    latitude: MinMax
    longitude: MinMax


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
