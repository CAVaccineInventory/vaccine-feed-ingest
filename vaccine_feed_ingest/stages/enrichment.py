"""Method for enriching location records after that are normalized"""
import json
import pathlib
from typing import Collection, Dict, Optional

import diskcache
import orjson
import phonenumbers
import pydantic
from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils.log import getLogger

from ..apis.geocodio import GeocodioAPI
from ..apis.placekey import PlacekeyAPI
from ..utils import misc, normalize
from . import outputs
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = getLogger(__file__)


PROVIDER_TAG = "_tag_provider"


def enrich_locations(
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    api_cache: Optional[diskcache.Cache] = None,
    enrich_apis: Optional[Collection[str]] = None,
    geocodio_apikey: Optional[str] = None,
    placekey_apikey: Optional[str] = None,
) -> bool:
    """Enrich locations in normalized input_dir and write them to output_dir"""
    enriched_locations = []

    if enrich_apis is None:
        enrich_apis = set()

    geocodio_api = None
    if "geocodio" in enrich_apis:
        if api_cache is not None and geocodio_apikey:
            geocodio_api = GeocodioAPI(api_cache, geocodio_apikey)
        else:
            logger.error("Skipping geocodio because geocodio api is not configured")

    placekey_api = None
    if "placekey" in enrich_apis:
        if api_cache is not None and placekey_apikey:
            placekey_api = PlacekeyAPI(api_cache, placekey_apikey)
        else:
            logger.error("Skipping placekey because placekey api is not configured")

    file_num = 0
    for file_num, filepath in enumerate(
        outputs.iter_data_paths(
            input_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]
        ),
        start=1,
    ):
        with filepath.open(mode="rb") as src_file:
            line_num = 0
            for line_num, line in enumerate(src_file, start=1):
                try:
                    loc_dict = orjson.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(
                        "Skipping source location because it is invalid json: %s\n%s",
                        line,
                        str(e),
                    )
                    continue

                try:
                    normalized_location = location.NormalizedLocation.parse_obj(
                        loc_dict
                    )
                except pydantic.ValidationError as e:
                    logger.warning(
                        "Skipping source location because it is invalid: %s\n%s",
                        line,
                        str(e),
                    )
                    continue

                _process_location(normalized_location)

                enriched_locations.append(normalized_location)

    _bulk_process_locations(
        enriched_locations,
        geocodio_api=geocodio_api,
        placekey_api=placekey_api,
    )

    enriched_locations = [
        loc for loc in enriched_locations if _is_loadable_location(loc)
    ]

    if not enriched_locations:
        logger.warning(
            "Processed %d lines across %d file(s) and found no loadable locations.",
            line_num,
            file_num,
        )
        return False

    suffix = STAGE_OUTPUT_SUFFIX[PipelineStage.ENRICH]
    dst_filepath = output_dir / f"locations{suffix}"

    with dst_filepath.open("wb") as dst_file:
        for loc in enriched_locations:
            loc_dict = loc.dict(exclude_none=True)
            loc_json = orjson.dumps(loc_dict)

            dst_file.write(loc_json)
            dst_file.write(b"\n")

    return True


def _process_location(loc: location.NormalizedLocation) -> None:
    """Run through all of the methods to enrich the location"""
    _add_provider_from_name(loc)
    _add_source_link(loc)
    _add_provider_tag(loc)

    _normalize_phone_format(loc)


def _bulk_process_locations(
    locs: Collection[location.NormalizedLocation],
    geocodio_api: Optional[GeocodioAPI] = None,
    placekey_api: Optional[PlacekeyAPI] = None,
) -> None:
    """Process locations all at once. Use for external apis with bulk apis"""
    _bulk_geocode(locs, geocodio_api)
    _bulk_add_placekey_link(locs, placekey_api)


def _is_loadable_location(loc: location.NormalizedLocation) -> bool:
    """Validate that the location is loadable after being enriched"""

    if not _valid_address(loc):
        logger.warning(
            "Skipping source location %s because its address could not be validated: %s",
            loc.id,
            loc.address,
        )
        return False

    if not loc.location:
        logger.warning(
            "Skipping source location %s because it does not have a location (lat/lng)",
            loc.id,
        )
        return False

    return True


def _generate_link_map(loc: location.NormalizedLocation) -> Dict[str, str]:
    """Return a map of authority to id value"""
    return {
        str(link.authority): link.id
        for link in loc.links or []
        if link.authority and link.id
    }


def _add_provider_from_name(loc: location.NormalizedLocation) -> None:
    """Add provider link from name if missing"""
    if not loc.name:
        return

    provider_tuple = normalize.provider_id_from_name(loc.name)

    if not provider_tuple:
        return

    provider_authority, provider_id = provider_tuple

    existing_links = _generate_link_map(loc)

    if str(provider_authority) not in existing_links:
        loc.links = [
            *(loc.links or []),
            location.Link(authority=provider_authority, id=provider_id),
        ]

    if not loc.parent_organization:
        loc.parent_organization = location.Organization(id=provider_authority)


def _add_source_link(loc: location.NormalizedLocation) -> None:
    """Add source link from source if missing"""
    if not loc.source:
        return

    if not loc.source.source or not loc.source.id:
        return

    existing_links = _generate_link_map(loc)

    if str(loc.source.source) in existing_links:
        return

    loc.links = [
        *(loc.links or []),
        location.Link(authority=loc.source.source, id=loc.source.id),
    ]


def _normalize_phone_format(loc: location.NormalizedLocation) -> None:
    """Normalize phone numbers into standard format"""
    if not loc.contact:
        return

    for contact in loc.contact:
        if not contact.phone:
            continue

        try:
            phone = phonenumbers.parse(contact.phone, "US")
        except phonenumbers.NumberParseException:
            logger.warning(
                "Invalid phone number for source location %s: %s",
                loc.id,
                contact.phone,
            )
            continue

        formatted_phone = phonenumbers.format_number(
            phone, phonenumbers.PhoneNumberFormat.NATIONAL
        )

        contact.phone = formatted_phone


def _add_provider_tag(loc: location.NormalizedLocation) -> None:
    """Add provider tag to concordances to use for matching"""
    if not loc.parent_organization:
        return

    if not loc.parent_organization.id:
        return

    existing_links = _generate_link_map(loc)

    if PROVIDER_TAG in existing_links:
        return

    provider_id = str(loc.parent_organization.id)

    loc.links = [
        *(loc.links or []),
        location.Link(authority=PROVIDER_TAG, id=provider_id),
    ]


def _bulk_geocode(
    locs: Collection[location.NormalizedLocation],
    geocodio_api: Optional[GeocodioAPI],
) -> None:
    """Geocode and fix addreses if missing"""
    if geocodio_api is None:
        return

    def _full_address(loc: location.NormalizedLocation) -> Optional[str]:
        # Only process if something is missing
        if loc.location and _valid_address(loc):
            return None

        # Skip if there is no partial address to work from
        if not loc.address:
            return None

        combined_address = []
        if loc.address.street1:
            combined_address.append(loc.address.street1)
        if loc.address.street2:
            combined_address.append(loc.address.street2)
        if loc.address.city:
            combined_address.append(loc.address.city)
        if loc.address.state:
            combined_address.append(loc.address.state)
        if loc.address.zip:
            combined_address.append(loc.address.zip)

        return ", ".join(combined_address)

    records = {}
    for loc in locs:
        full_address = _full_address(loc)
        if full_address:
            records[loc.id] = full_address

    if not records:
        return

    for chunked_records in misc.dict_batch(records, 5000):
        logger.info("attempting to geocode %d locations", len(chunked_records))
        places = geocodio_api.batch_geocode(chunked_records)

        if not places:
            logger.info(
                "No places returned from geocode for %s records", len(chunked_records)
            )
            continue

        for loc in locs:
            place_results = places.get(loc.id)

            if not place_results:
                continue

            # Only trust the result if exactly one is returned
            if len(place_results) > 1:
                logger.info(
                    "More than one geocode result returned for %s. Skipping geocoding.",
                    loc.id,
                )
                continue

            place_result = place_results[0]

            if "location" not in place_result:
                logger.warning(
                    "No lat-lng returned from geocode for %s. Skipping geocoding.",
                    loc.id,
                )
                continue

            address_components = place_result.get("address_components")

            if not address_components:
                logger.warning(
                    "No address components returned from geocode for %s. Skipping geocoding.",
                    loc.id,
                )
                continue

            if "formatted_street" not in address_components:
                logger.warning(
                    "No formatted_street returned from geocode for %s. Skipping geocoding.",
                    loc.id,
                )
                continue

            if "city" not in address_components:
                logger.warning(
                    "No city returned from geocode for %s. Skipping geocoding.",
                    loc.id,
                )
                continue

            if "state" not in address_components:
                logger.warning(
                    "No state returned from geocode for %s. Skipping geocoding.",
                    loc.id,
                )
                continue

            if "zip" not in address_components:
                logger.warning(
                    "No zip returned from geocode for %s. Skipping geocoding.",
                    loc.id,
                )
                continue

            geocode_location = place_result["location"]

            loc.location = location.LatLng(
                latitude=geocode_location["lat"],
                longitude=geocode_location["lng"],
            )

            if address_components := place_result.get("address_components"):
                street2 = None
                if (
                    "secondaryunit" in address_components
                    and "secondarynumber" in address_components
                ):
                    street2 = " ".join(
                        [
                            address_components["secondaryunit"],
                            address_components["secondarynumber"],
                        ]
                    )

                loc.address = location.Address(
                    street1=address_components.get("formatted_street"),
                    street2=street2,
                    city=address_components.get("city"),
                    state=address_components.get("state"),
                    zip=address_components.get("zip"),
                )


def _bulk_add_placekey_link(
    locs: Collection[location.NormalizedLocation],
    placekey_api: Optional[PlacekeyAPI],
) -> None:
    """Add placekey concordance to location if we can"""
    if placekey_api is None:
        return

    def _placekey_record(loc: location.NormalizedLocation) -> Optional[dict]:
        if not loc.location:
            return None

        if not _valid_address(loc):
            return None

        street_address = [loc.address.street1]
        if loc.address.street2:
            street_address.append(loc.address.street2)

        return {
            "latitude": loc.location.latitude,
            "longitude": loc.location.longitude,
            "location_name": loc.name,
            "street_address": ", ".join(street_address),
            "city": loc.address.city,
            "region": loc.address.state,
            "postal_code": loc.address.zip,
            "iso_country_code": "US",
        }

    records = {}
    for loc in locs:
        record = _placekey_record(loc)
        if record:
            records[loc.id] = record

    placekeys = placekey_api.lookup_placekeys(records)

    if not placekeys:
        return

    for loc in locs:
        placekey_id = placekeys.get(loc.id)

        if not placekey_id:
            continue

        # Verify "what" part of placekey is specified or else placekey isn't specific enough
        if "@" not in placekey_id or placekey_id.startswith("@"):
            continue

        loc.links = [
            *(loc.links or []),
            location.Link(authority="placekey", id=placekey_id),
        ]


def _valid_address(loc: location.NormalizedLocation) -> bool:
    """Verify that address has all of the required components"""
    if not loc.address:
        return False

    if not loc.address.street1:
        return False

    if not loc.address.city:
        return False

    if not loc.address.state:
        return False

    if not loc.address.zip:
        return False

    return True
