"""Method for enriching location records after that are normalized"""
import pathlib
from typing import Dict, Optional

import phonenumbers
import pydantic
from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils.log import getLogger

from ..utils import normalize
from . import outputs
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = getLogger(__file__)


def enrich_locations(input_dir: pathlib.Path, output_dir: pathlib.Path) -> bool:
    """Enrich locations in normalized input_dir and write them to output_dir"""
    enriched_locations = []

    for filepath in outputs.iter_data_paths(
        input_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.NORMALIZE]
    ):
        with filepath.open() as src_file:
            for line in src_file:
                try:
                    normalized_location = location.NormalizedLocation.parse_raw(line)
                except pydantic.ValidationError as e:
                    logger.warning(
                        "Skipping source location because it is invalid: %s\n%s",
                        line,
                        str(e),
                    )
                    continue

                enriched_location = _process_location(normalized_location)

                if not enriched_location:
                    continue

                enriched_locations.append(enriched_location)

    if not enriched_locations:
        return False

    suffix = STAGE_OUTPUT_SUFFIX[PipelineStage.ENRICH]
    dst_filepath = output_dir / f"locations{suffix}"

    with dst_filepath.open("w") as dst_file:
        for loc in enriched_locations:
            dst_file.write(loc.json())
            dst_file.write("\n")

    return True


def _process_location(
    normalized_location: location.NormalizedLocation,
) -> Optional[location.NormalizedLocation]:
    """Run through all of the methods to enrich the location"""
    enriched_location = normalized_location.copy()

    _add_provider_from_name(enriched_location)
    _add_source_link(enriched_location)

    _normalize_phone_format(enriched_location)

    if not _valid_address(enriched_location):
        return None

    if not enriched_location.location:
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
