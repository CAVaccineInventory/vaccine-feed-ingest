import logging
import pathlib
from typing import Iterator, List, Optional

import jellyfish
import pydantic
import rtree
import shapely.geometry
import urllib3
import us
from vaccine_feed_ingest_schema import load, location

from .. import vial
from ..utils.match import canonicalize_address, get_full_address
from . import outputs
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = logging.getLogger("load")


def run_load_to_vial(
    vial_http: urllib3.connectionpool.ConnectionPool,
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    import_run_id: str,
    locations: Optional[rtree.index.Index],
    enable_match: bool = True,
    enable_create: bool = False,
    candidate_distance: float = 0.6,
    dry_run: bool = False,
) -> Optional[List[load.ImportSourceLocation]]:
    """Load source to vial source locations"""
    ennrich_run_dir = outputs.find_latest_run_dir(
        output_dir, site_dir.parent.name, site_dir.name, PipelineStage.ENRICH
    )
    if not ennrich_run_dir:
        logger.warning(
            "Skipping load for %s because there is no data from enrich stage",
            site_dir.name,
        )
        return None

    if not outputs.data_exists(
        ennrich_run_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.ENRICH]
    ):
        logger.warning("No enriched data available to load for %s.", site_dir.name)
        return None

    num_imported_locations = 0

    for filepath in outputs.iter_data_paths(
        ennrich_run_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.ENRICH]
    ):
        import_locations = []
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

                match_action = None
                if (enable_match or enable_create) and locations is not None:
                    match_action = _match_source_to_existing_locations(
                        normalized_location,
                        locations,
                        candidate_distance,
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

    return import_locations


def _find_candidates(
    source: location.NormalizedLocation,
    existing: rtree.index.Index,
    candidate_distance: float,
) -> Iterator[dict]:
    """Return a slice of existing locations"""
    src_point = shapely.geometry.Point(
        source.location.longitude,
        source.location.latitude,
    )

    search_bounds = src_point.buffer(candidate_distance).bounds

    yield from existing.intersection(search_bounds, objects="raw")


def _is_different(source: location.NormalizedLocation, candidate: dict) -> bool:
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

    return False


def _is_match(source: location.NormalizedLocation, candidate: dict) -> bool:
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
    source: location.NormalizedLocation,
    existing: rtree.index.Index,
    candidate_distance: float,
    enable_match: bool = True,
    enable_create: bool = False,
) -> Optional[load.ImportMatchAction]:
    """Attempt to match source location to existing locations"""
    if not source.location:
        return None

    candidates = list(_find_candidates(source, existing, candidate_distance))

    if not candidates:
        logger.info("%s is a new location - nothing close", source.name)
        if enable_create:
            return load.ImportMatchAction(action="new")
        else:
            return None

    # Filter out candidates that are too different to be a match
    candidates = [loc for loc in candidates if not _is_different(source, loc)]

    if not candidates:
        logger.info("%s is a new location", source.name)
        if enable_create:
            return load.ImportMatchAction(action="new")
        else:
            return None

    # Filter to candidates that are similar enough to be the same
    candidates = [loc for loc in candidates if _is_match(source, loc)]

    # If there is one remaining high confidant match then use it.
    if len(candidates) == 1:
        logger.info("%s is an existing location", source.name)
        if enable_match:
            return load.ImportMatchAction(
                action="existing",
                id=candidates[0]["properties"]["id"],
            )
        else:
            return None

    logger.info("%d matches, not sure about %s", len(candidates), source.name)
    return None


def _create_import_location(
    normalized_record: location.NormalizedLocation,
    match_action: Optional[load.ImportMatchAction] = None,
) -> load.ImportSourceLocation:
    """Transform normalized record into import record"""
    import_location = load.ImportSourceLocation(
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
