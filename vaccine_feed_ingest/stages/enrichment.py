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

from ..apis.placekey import PlacekeyAPI
from ..utils import normalize
from . import outputs
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = getLogger(__file__)


PROVIDER_TAG = "_tag_provider"


def enrich_locations(
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    api_cache: Optional[diskcache.Cache] = None,
    enrich_apis: Optional[Collection[str]] = None,
    placekey_apikey: Optional[str] = None,
) -> bool:
    """Enrich locations in normalized input_dir and write them to output_dir"""
    enriched_locations = []

    if enrich_apis is None:
        enrich_apis = set()

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

                enriched_location = _process_location(
                    normalized_location,
                    placekey_api=placekey_api,
                )

                if not enriched_location:
                    continue

                enriched_locations.append(enriched_location)

    if not enriched_locations:
        logger.warning(
            "Processed %d lines across %d file(s). Despite this, found no enriched locations.",
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


def _process_location(
    normalized_location: location.NormalizedLocation,
    placekey_api: Optional[PlacekeyAPI] = None,
) -> Optional[location.NormalizedLocation]:
    """Run through all of the methods to enrich the location"""
    enriched_location = normalized_location.copy()

    _add_provider_from_name(enriched_location)
    _add_source_link(enriched_location)
    _add_provider_tag(enriched_location)

    _normalize_phone_format(enriched_location)

    _add_placekey_link(enriched_location, placekey_api)

    if not _valid_address(enriched_location):
        logger.warning(
            "Skipping source location %s because its address could not be validated: %s",
            normalized_location.id,
            normalized_location.address,
        )
        return None

    if not enriched_location.location:
        logger.warning(
            "Skipping source location %s because it does not have a location (lat/lng)",
            normalized_location.id,
        )
        return None

    return enriched_location


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


def _add_placekey_link(
    loc: location.NormalizedLocation,
    placekey_api: Optional[PlacekeyAPI],
) -> None:
    """Add placekey concordance to location if we can"""
    if placekey_api is None:
        return

    if not loc.location:
        return

    if not _valid_address(loc):
        return

    street_address = [loc.address.street1]
    if loc.address.street2:
        street_address.append(loc.address.street2)

    loc_placekey = placekey_api.lookup_placekey(
        loc.location.latitude,
        loc.location.longitude,
        loc.name,
        ", ".join(street_address),
        loc.address.city,
        loc.address.state,
        loc.address.zip,
    )

    if not loc_placekey:
        return

    # Verify "what" part of placekey is specified or else placekey isn't specific enough
    if "@" not in loc_placekey or loc_placekey.startswith("@"):
        return

    loc.links = [
        *(loc.links or []),
        location.Link(authority="placekey", id=loc_placekey),
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
