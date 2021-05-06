"""
Utilities for the parse step
"""
import re
from typing import Optional

import usaddress
from vaccine_feed_ingest_schema.location import Address


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


def parse_address(full_address: str) -> Optional[Address]:
    """Parse an address string into address components"""

    address_record, address_type = usaddress.tag(
        full_address,
        tag_mapping={
            "Recipient": "recipient",
            "AddressNumber": "street1",
            "AddressNumberPrefix": "street1",
            "AddressNumberSuffix": "street1",
            "StreetName": "street1",
            "StreetNamePreDirectional": "street1",
            "StreetNamePreModifier": "street1",
            "StreetNamePreType": "street1",
            "StreetNamePostDirectional": "street1",
            "StreetNamePostModifier": "street1",
            "StreetNamePostType": "street1",
            "CornerOf": "street1",
            "IntersectionSeparator": "street1",
            "LandmarkName": "street1",
            "USPSBoxGroupID": "street1",
            "USPSBoxGroupType": "street1",
            "USPSBoxID": "street1",
            "USPSBoxType": "street1",
            "BuildingName": "street2",
            "OccupancyType": "street2",
            "OccupancyIdentifier": "street2",
            "SubaddressIdentifier": "street2",
            "SubaddressType": "street2",
            "PlaceName": "city",
            "StateName": "state",
            "ZipCode": "zip",
        },
    )

    address = Address(
        street1=address_record.get("street1"),
        street2=address_record.get("street2"),
        city=address_record.get("city"),
        state=address_record.get("state"),
        zip=address_record.get("zip"),
    )

    return address
