"""
Utilities for the parse step
"""
import re

from .log import getLogger

logger = getLogger(__file__)


def location_id_from_name(name: str) -> str:
    """Get a stable ID for a location from its name.

    When nothing else is available, names can be used as an ID.
    We don't want duplicate entries for the same location. If the
    same location is listed multiple times with minor differences
    in the name (extra space, etc), we want to produce a single,
    consistent ID.

    """

    # trim surrounding whitespace and lower-case.
    id = name.strip().lower()

    # Only keep alphanumeric characters, hyphens, and spaces.
    id = re.sub(r"[^a-z0-9 -]", "", id)

    # Replace interior whitespace with underscores
    id = re.sub(r"[_ -]+", "_", id)

    return id
