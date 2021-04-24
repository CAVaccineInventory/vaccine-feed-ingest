import logging
import pathlib
from typing import Iterator, Optional

import jellyfish
import pydantic
import rtree
import shapely.geometry
import urllib3
import us
from vaccine_feed_ingest import vial
from vaccine_feed_ingest.schema import schema

from ..utils.match import canonicalize_address, get_full_address
from . import outputs
from .common import PipelineStage

logger = logging.getLogger("load")


# Collect locations that are within .6 degrees = 66.6 km = 41 mi
CANDIDATE_DEGREES_DISTANCE = 0.6


def run_load_to_vial(
    vial_http: urllib3.connectionpool.ConnectionPool,
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    import_run_id: str,
    locations: Optional[rtree.index.Index],
    enable_match: bool = True,
    enable_create: bool = False,
    dry_run: bool = False,
) -> bool:
    """Load source to vial source locations"""
    normalize_run_dir = outputs.find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.NORMALIZE
    )
    if not normalize_run_dir:
        logger.warning(
            "Skipping load for %s because there is no data from normalize stage",
            site_dir.name,
        )
        return False

    if not outputs.data_exists(normalize_run_dir):
        logger.warning("No normalize data available to load for %s.", site_dir.name)
        return False

    num_imported_locations = 0

    for filepath in outputs.iter_data_paths(normalize_run_dir):
        if not filepath.name.endswith(".normalized.ndjson"):
            continue

        import_locations = []
        with filepath.open("rb") as src_file:
            for line in src_file:
                try:
                    normalized_location = schema.NormalizedLocation.parse_raw(line)
                except pydantic.ValidationError:
                    logger.warning(
                        "Skipping source location because it is invalid: %s",
                        line,
                        exc_info=True,
                    )
                    continue

                match_action = None
                if (enable_match or enable_create) and locations is not None:
                    match_action = _match_source_to_existing_locations(
                        normalized_location,
                        locations,
                        enable_match=enable_match,
                        enable_create=enable_create,
                    )

                import_location = _create_import_location(
                    normalized_location, match_action=match_action
                )

                import_locations.append(import_location)

        if not import_locations:
            logger.warning(
                "No locations to import in %s in %s",
                filepath.name,
                site_dir.name,
            )
            continue

        if not dry_run:
            import_resp = vial.import_source_locations(
                vial_http, import_run_id, import_locations
            )

            if import_resp.status != 200:
                logger.warning(
                    "Failed to import source locations for %s in %s: %s",
                    filepath.name,
                    site_dir.name,
                    import_resp.data[:100],
                )
                continue

        num_imported_locations += len(import_locations)

    logger.info(
        "Imported %d source locations for %s",
        num_imported_locations,
        site_dir.name,
    )

    return bool(num_imported_locations)


def _find_candidates(
    source: schema.NormalizedLocation,
    existing: rtree.index.Index,
) -> Iterator[dict]:
    """Return a slice of existing locations"""
    src_point = shapely.geometry.Point(
        source.location.longitude,
        source.location.latitude,
    )

    search_bounds = src_point.buffer(CANDIDATE_DEGREES_DISTANCE).bounds

    result = existing.intersection(search_bounds, objects=True)
    for row in result:
        yield row.object


def _is_different(source: schema.NormalizedLocation, candidate: dict) -> bool:
    """Return True if candidate is so different it couldn't be a match"""
    candidate_props = candidate.get("properties", {})

    # Must be in same state to be considered the same location
    if source.address and source.address.state and candidate_props.get("state"):
        src_state = us.states.lookup(source.address.state)
        cand_state = us.states.lookup(candidate_props["state"])

        if src_state != cand_state:
            return True

    # City name must be slightly similiar to match.
    if source.address and source.address.city and candidate_props.get("city"):
        src_city = source.address.city
        cand_city = candidate_props["city"]

        if jellyfish.jaro_winkler(src_city, cand_city) < 0.1:
            return True

    # Parent organization must be slightly similar to match.
    if source.parent_organization and candidate_props.get("provider"):
        src_org = source.parent_organization.name or source.parent_organization.id
        cand_org = candidate_props["provider"].get("name")

        if src_org and cand_org and jellyfish.jaro_winkler(src_org, cand_org) < 0.1:
            return True

    # Must not have any conflicting links/concordances
    if source.links and candidate_props.get("concordances"):
        src_link_map = {link.authority: link.id for link in source.links}

        for link in candidate_props["concordances"]:
            if ":" not in link:
                continue

            link_authority, link_id = link.split(":", maxsplit=1)
            if not link_id:
                continue

            src_link_id = src_link_map.get(link_authority)

            if not src_link_id:
                continue

            if src_link_id != link_id:
                return True

    return False


def _is_match(source: schema.NormalizedLocation, candidate: dict) -> bool:
    """Return True if candidate is so similar it must be a match"""
    source_links = (
        set("{}:{}".format(link.authority, link.id) for link in source.links)
        if source.links
        else set()
    )

    candidate = candidate["properties"]
    candidate_links = (
        set(candidate["concordances"]) if "concordances" in candidate else set()
    )
    shared_links = source_links.intersection(candidate_links)

    if len(shared_links) > 0:
        # Shared concordances, mark as match
        return True

    if candidate["full_address"] is not None and source.address is not None:
        if canonicalize_address(
            get_full_address(source.address)
        ) == canonicalize_address(candidate["full_address"]):
            # Canonicalized address matches, mark as match
            return True

    return False


def _match_source_to_existing_locations(
    source: schema.NormalizedLocation,
    existing: rtree.index.Index,
    enable_match: bool = True,
    enable_create: bool = False,
) -> Optional[schema.ImportMatchAction]:
    """Attempt to match source location to existing locations"""
    if not source.location:
        return None

    candidates = list(_find_candidates(source, existing))

    if not candidates:
        logger.info("%s is a new location - nothing close", source.name)
        if enable_create:
            return schema.ImportMatchAction(action="new")
        else:
            return None

    # Filter out candidates that are too different to be a match
    candidates = [loc for loc in candidates if not _is_different(source, loc)]

    if not candidates:
        logger.info("%s is a new location", source.name)
        if enable_create:
            return schema.ImportMatchAction(action="new")
        else:
            return None

    # Filter to candidates that are similar enough to be the same
    candidates = [loc for loc in candidates if _is_match(source, loc)]

    # If there is one remaining high confidant match then use it.
    if len(candidates) == 1:
        logger.info("%s is an existing location", source.name)
        if enable_match:
            return schema.ImportMatchAction(
                action="existing",
                id=candidates[0]["properties"]["id"],
            )
        else:
            return None

    logger.info("%d matches, not sure about %s", len(candidates), source.name)
    return None


def _create_import_location(
    normalized_record: schema.NormalizedLocation,
    match_action: Optional[schema.ImportMatchAction] = None,
) -> schema.ImportSourceLocation:
    """Transform normalized record into import record"""
    import_location = schema.ImportSourceLocation(
        source_uid=normalized_record.id,
        source_name=normalized_record.source.source,
        import_json=normalized_record,
    )

    if normalized_record.name:
        import_location.name = normalized_record.name

    if normalized_record.location:
        import_location.latitude = normalized_record.location.latitude
        import_location.longitude = normalized_record.location.longitude

    if match_action:
        import_location.match = match_action

    return import_location
