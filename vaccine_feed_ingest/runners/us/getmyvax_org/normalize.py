#!/usr/bin/env python3

import datetime
import pathlib
import sys
from typing import List, Optional, Tuple

import orjson
import pydantic
import us
from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


SOURCE_NAME = "getmyvax_org"
LOCATIONS_URL = "https://getmyvax.org/api/edge/locations.ndjson"

VACCINE_MAPPING = {
    "moderna": location.VaccineType.MODERNA,
    "pfizer": location.VaccineType.PFIZER_BIONTECH,
    "jj": location.VaccineType.JOHNSON_JOHNSON_JANSSEN,
}


PROVIDER_MAPPING = {
    "albertsons_acme": location.VaccineProvider.ACME,
    "albertsons_amigos": location.VaccineProvider.AMIGOS,
    "albertsons_carrs": location.VaccineProvider.CARRS,
    "albertsons_haggen": location.VaccineProvider.HAGGEN,
    "albertsons_jewelosco": location.VaccineProvider.JEWEL_OSCO,
    "albertsons_market_street": location.VaccineProvider.MARKET_STREET,
    "albertsons_market": location.VaccineProvider.ALBERTSONS_MARKET,
    "albertsons_pak_n_save": location.VaccineProvider.PAK_N_SAVE,
    "albertsons_pavilions": location.VaccineProvider.PAVILIONS,
    "albertsons_randalls": location.VaccineProvider.RANDALLS,
    "albertsons_safeway": location.VaccineProvider.SAFEWAY,
    "albertsons_shaws": location.VaccineProvider.SHAWS,
    "albertsons_star_market": location.VaccineProvider.STAR_MARKET,
    "albertsons_tom_thumb": location.VaccineProvider.TOM_THUMB,
    "albertsons_united": location.VaccineProvider.UNITED_SUPERMARKET,
    "albertsons_vons": location.VaccineProvider.VONS,
    "albertsons": location.VaccineProvider.ALBERTSONS,
    "alliancerx_walgreens_prime": location.VaccineProvider.WALGREENS,
    "community_a_walgreens_pharmacy": location.VaccineProvider.WALGREENS,
    "costco": location.VaccineProvider.COSTCO,
    "cvs": location.VaccineProvider.CVS,
    "fresco_y_mas": location.VaccineProvider.FRESCO_Y_MAS,
    "harveys": location.VaccineProvider.HARVEYS,
    "health_mart_health_mart": location.VaccineProvider.HEALTH_MART,
    "health_mart": location.VaccineProvider.HEALTH_MART,
    "heb": location.VaccineProvider.HEB,
    "hyvee": location.VaccineProvider.HY_VEE,
    "kroger_bakers": location.VaccineProvider.BAKERS,
    "kroger_citymarket": location.VaccineProvider.CITY_MARKET,
    "kroger_covid": location.VaccineProvider.KROGER,
    "kroger_dillons": location.VaccineProvider.DILLONS,
    "kroger_fred": location.VaccineProvider.FRED_MEYER,
    "kroger_frys": location.VaccineProvider.FRYS,
    "kroger_gerbes": location.VaccineProvider.GERBES,
    "kroger_hart": location.VaccineProvider.HART,
    "kroger_jayc": location.VaccineProvider.JAYC,
    "kroger_kingsoopers": location.VaccineProvider.KING_SOOPERS,
    "kroger_marianos": location.VaccineProvider.MARIANOS,
    "kroger_metro_market": location.VaccineProvider.METRO_MARKET,
    "kroger_payless": location.VaccineProvider.PAY_LESS,
    "kroger_pick_n_save": location.VaccineProvider.PICK_N_SAVE,
    "kroger_qfc": location.VaccineProvider.QFC,
    "kroger_ralphs": location.VaccineProvider.RALPHS,
    "kroger_smiths": location.VaccineProvider.SMITHS,
    "kroger_the_little_clinic": location.VaccineProvider.LITTLE_CLINIC,
    "kroger": location.VaccineProvider.KROGER,
    "kta_super_stores": location.VaccineProvider.KTA_SUPER_STORES,
    "pharmaca": location.VaccineProvider.PHARMACA,
    "price_chopper_market_32": location.VaccineProvider.MARKET_32,
    "price_chopper_market_bistro": location.VaccineProvider.MARKET_BISTRO,
    "price_chopper": location.VaccineProvider.PRICE_CHOPPER,
    "publix": location.VaccineProvider.PUBLIX,
    "rite_aid": location.VaccineProvider.RITE_AID,
    "riteaid": location.VaccineProvider.RITE_AID,
    "safeway": location.VaccineProvider.SAFEWAY,
    "sams_club_sams_club": location.VaccineProvider.SAMS,
    "sams_club": location.VaccineProvider.SAMS,
    "southeastern_grocers_fresco_y_mas": location.VaccineProvider.FRESCO_Y_MAS,
    "southeastern_grocers_harveys": location.VaccineProvider.HARVEYS,
    "southeastern_grocers_winn_dixie": location.VaccineProvider.WINN_DIXIE,
    "thrifty_white": location.VaccineProvider.THRIFTY_WHITE,
    "walgreens_duane_reade": location.VaccineProvider.DUANE_READE,
    "walgreens_specialty_pharmacy_of_puerto_rico": location.VaccineProvider.WALGREENS,
    "walgreens_specialty_pharmacy": location.VaccineProvider.WALGREENS,
    "walgreens": location.VaccineProvider.WALGREENS,
    "walmart_walmart": location.VaccineProvider.WALMART,
    "walmart": location.VaccineProvider.WALMART,
    "wegmans": location.VaccineProvider.WEGMANS,
    "weis_weis": location.VaccineProvider.WEIS,
    "weis": location.VaccineProvider.WEIS,
    "winn_dixie": location.VaccineProvider.WINN_DIXIE,
}


class BaseModel(pydantic.BaseModel):
    """BaseModel for all schema to inherit from."""

    class Config:
        # Change this to "forbid" when testing to catch issues
        extra = "ignore"

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
    sources: List[str]
    valid_at: datetime.datetime
    checked_at: datetime.datetime
    changed_at: datetime.datetime
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
    description: Optional[str]
    requires_waitlist: bool
    meta: Optional[dict]
    created_at: datetime.datetime
    updated_at: datetime.datetime
    availability: Optional[Availability]
    external_ids: Optional[List[Tuple[str, str]]]


def process_line(line: bytes, timestamp: datetime.datetime) -> bytes:
    in_loc_dict = orjson.loads(line)
    in_loc = GMVLocation.parse_obj(in_loc_dict)

    out_loc = normalize_location(in_loc, timestamp)

    out_loc_dict = out_loc.dict(exclude_none=True)
    return orjson.dumps(out_loc_dict, option=orjson.OPT_APPEND_NEWLINE)


def _get_address(loc: GMVLocation) -> Optional[location.Address]:
    if not loc.address_lines and not loc.city and not loc.state and not loc.postal_code:
        logger.info("No address for location %s (%s)", loc.id, loc.name)
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
            logger.warning("Invalid state %s for %s (%s)", loc.state, loc.id, loc.name)

    postal_code = None
    # Handle invalid postal codes that are less than 5 digits
    if loc.postal_code:
        if len(loc.postal_code) >= 5:
            postal_code = loc.postal_code
        else:
            logger.warning(
                "Invalid postal code %s for %s (%s)", loc.postal_code, loc.id, loc.name
            )

    return location.Address(
        street1=street1,
        street2=street2,
        city=loc.city,
        state=state_abbr,
        zip=postal_code,
    )


def _get_lat_lng(loc: GMVLocation) -> Optional[location.LatLng]:
    if not loc.position:
        logger.debug("No lat-lng for location %s (%s)", loc.id, loc.name)
        return None

    # Skip positions that are missing a value
    if not loc.position.latitude or not loc.position.longitude:
        logger.warning("Skipping position with missing coordinates")
        return None

    return location.LatLng(
        latitude=loc.position.latitude,
        longitude=loc.position.longitude,
    )


def _get_contacts(loc: GMVLocation) -> Optional[List[location.Contact]]:
    contacts = []

    if loc.info_phone:
        contacts.append(
            location.Contact(
                contact_type=location.ContactType.GENERAL,
                phone=loc.info_phone,
            )
        )

    if loc.info_url:
        contacts.append(
            location.Contact(
                contact_type=location.ContactType.GENERAL,
                website=loc.info_url,
            )
        )

    if loc.booking_phone:
        contacts.append(
            location.Contact(
                contact_type=location.ContactType.BOOKING,
                phone=loc.booking_phone,
            )
        )

    if loc.booking_url:
        contacts.append(
            location.Contact(
                contact_type=location.ContactType.BOOKING,
                website=loc.booking_url,
            )
        )

    if not contacts:
        return None

    return contacts


def _get_availability(loc: GMVLocation) -> Optional[location.Availability]:
    if not loc.availability:
        return None

    if loc.availability.available == "UNKNOWN":
        return None

    return location.Availability(appointments=True)


def _get_inventory(loc: GMVLocation) -> Optional[List[location.Vaccine]]:
    if not loc.availability:
        return None

    vaccines = set()
    for product in loc.availability.products or []:
        if vaccine_type := VACCINE_MAPPING.get(product):
            vaccines.add(vaccine_type)
        else:
            logger.info("Unrecognized vaccine for product %s", product)

    if not vaccines:
        return None

    return [location.Vaccine(vaccine=vaccine) for vaccine in vaccines]


def _get_parent_organization(loc: GMVLocation) -> Optional[location.Organization]:
    provider = PROVIDER_MAPPING.get(loc.provider)

    if not provider:
        return None

    return location.Organization(id=provider)


def _get_links(loc: GMVLocation) -> Optional[List[location.Link]]:
    if not loc.external_ids:
        return None

    links = []

    for provider_id, store_id in loc.external_ids:
        if not store_id:
            logger.info("Missing value for external_id %s", provider_id)
            continue

        authority = None

        if provider_id == "vaccinespotter":
            authority = "vaccinespotter_org"

        elif provider_id == "vtrcks":
            authority = location.LocationAuthority.VTRCKS

        elif provider_id.startswith("univaf_"):
            # Skip versioned univaf ids until we figure out what to do with them.
            continue

        elif provider_id in ("njiis_covid", "comassvax"):
            # Skip because their ids are just addresses
            continue

        elif provider_id in ("centura_driveup_event", "denver_ball_arena"):
            # Skip because I am not sure if the ids are of good quality or useful
            continue

        elif (
            provider_id.endswith("_powered_by_walgreens")
            or provider_id.endswith("_a_walgreens_pharmacy")
            or provider_id.endswith("_a_walgreens_rx")
        ):
            # These stores keep their custom names, but have walgreens store ids
            authority = location.VaccineProvider.WALGREENS

        else:
            authority = PROVIDER_MAPPING.get(provider_id)

        if not authority:
            logger.info(
                "Unrecognized provider for external_id %s:%s", provider_id, store_id
            )
            continue

        links.append(location.Link(authority=authority, id=str(store_id)))

    if not links:
        return None

    return links


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
        access=None,
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
        filepath_stem = in_filepath.name[: -len(".parsed.ndjson")]
        out_filepath = output_dir / f"{filepath_stem}.normalized.ndjson"
        with out_filepath.open("wb") as out_file:
            for line in in_file:
                out_loc_ndjson = process_line(line, timestamp)
                out_file.write(out_loc_ndjson)
