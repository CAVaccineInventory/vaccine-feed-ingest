#!/usr/bin/env python3

import datetime
import pathlib
import sys
from typing import List, Optional

import orjson
import pydantic
import us
from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


SOURCE_NAME = "getmyvax_org"
LOCATIONS_URL = "https://getmyvax.org/api/edge/locations.ndjson"


class BaseModel(pydantic.BaseModel):
    """BaseModel for all schema to inherit from."""

    class Config:
        # Fail if an attribute that doesn't exist is added.
        # This helps reduce typos.
        extra = "forbid"

        # Store enums as string values.
        # This helps when using exporting models with enums
        use_enum_values = True


class CapacityItem(BaseModel):
    date: datetime.date
    dose: Optional[str]
    products: Optional[List[str]]
    available: str
    available_count: Optional[int]
    unavailable_count: Optional[int]


class Availability(BaseModel):
    source: str
    valid_at: datetime.datetime
    checked_at: datetime.datetime
    available: str
    available_count: Optional[int]
    capacity: Optional[List[CapacityItem]]
    products: Optional[List[str]]
    doses: Optional[List[str]]


class Position(BaseModel):
    latitude: float
    longitude: float


class GMVLocation(BaseModel):
    id: str
    provider: str
    location_type: str
    name: str
    address_lines: List[str]
    city: Optional[str]
    state: str
    postal_code: Optional[str]
    county: Optional[str]
    position: Optional[Position]
    info_phone: Optional[str]
    info_url: Optional[str]
    booking_phone: Optional[str]
    booking_url: Optional[str]
    eligibility: Optional[str]
    description: Optional[str]
    requires_waitlist: bool
    meta: Optional[dict]
    created_at: datetime.datetime
    updated_at: datetime.datetime
    availability: Optional[Availability]
    external_ids: Optional[dict]


def process_line(line: bytes, timestamp: datetime.datetime) -> bytes:
    in_loc_dict = orjson.loads(line)
    in_loc = GMVLocation.parse_obj(in_loc_dict)

    out_loc = normalize_location(in_loc, timestamp)

    out_loc_dict = out_loc.dict(exclude_none=True)
    return orjson.dumps(out_loc_dict, option=orjson.OPT_APPEND_NEWLINE)


def _get_address(loc: GMVLocation) -> Optional[location.Address]:
    if not loc.address_lines and not loc.city and not loc.state and not loc.postal_code:
        return None

    street1 = None
    if loc.address_lines:
        street1 = loc.address_lines[0]

    street2 = None
    if len(loc.address_lines) > 1:
        street2 = ", ".join(loc.address_lines[1:])

    state_abbr = None
    if loc.state:
        if state := us.states.lookup(loc.state):
            state_abbr = state.abbr
        else:
            logger.warning("Invalid state %s", loc.state)

    postal_code = None
    # Handle invalid postal codes that are less than 5 digits
    if loc.postal_code:
        if len(loc.postal_code) >= 5:
            postal_code = loc.postal_code
        else:
            logger.warning("Invalid postal code %s", loc.postal_code)

    return location.Address(
        street1=street1,
        street2=street2,
        city=loc.city,
        state=state_abbr,
        zip=postal_code,
    )


def _get_lat_lng(loc: GMVLocation) -> Optional[location.LatLng]:
    pass


def _get_contacts(loc: GMVLocation) -> Optional[List[location.Contact]]:
    pass


def _get_availability(loc: GMVLocation) -> Optional[location.Availability]:
    pass


def _get_inventory(loc: GMVLocation) -> Optional[List[location.Vaccine]]:
    pass


def _get_access(loc: GMVLocation) -> Optional[location.Access]:
    pass


def _get_parent_organization(loc: GMVLocation) -> Optional[location.Organization]:
    pass


def _get_links(loc: GMVLocation) -> Optional[List[location.Link]]:
    pass


def normalize_location(
    loc: GMVLocation, timestamp: datetime.datetime
) -> location.NormalizedLocation:
    return location.NormalizedLocation(
        id=f"{SOURCE_NAME}:{loc.id}",
        name=loc.name,
        address=_get_address(loc),
        location=_get_lat_lng(loc),
        contact=_get_contacts(loc),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=_get_availability(loc),
        inventory=_get_inventory(loc),
        access=_get_access(loc),
        parent_organization=_get_parent_organization(loc),
        links=_get_links(loc),
        notes=None,
        active=None,
        source=location.Source(
            source=SOURCE_NAME,
            id=loc.id,
            fetched_from_uri=LOCATIONS_URL,
            fetched_at=timestamp,
            published_at=loc.updated_at,
            data=loc.dict(exclude_none=True),
        ),
    )


output_dir = pathlib.Path(sys.argv[1]) if len(sys.argv) >= 2 else None
if output_dir is None:
    logger.error("Must pass an output_dir as first argument")
    sys.exit(1)

input_dir = pathlib.Path(sys.argv[2]) if len(sys.argv) >= 3 else None
if input_dir is None:
    logger.error("Must pass an input_dir as second argument")
    sys.exit(1)

for in_filepath in input_dir.iterdir():
    if not in_filepath.name.endswith(".parsed.ndjson"):
        continue

    logger.info(f"Normalizing locations in {in_filepath.name}")

    timestamp = datetime.datetime.now()

    with in_filepath.open("rb") as in_file:
        out_filepath = output_dir / f"{in_filepath.stem}.normalized.ndjson"
        with out_filepath.open("wb") as out_file:
            for line in in_file:
                out_loc_ndjson = process_line(line, timestamp)
                out_file.write(out_loc_ndjson)
