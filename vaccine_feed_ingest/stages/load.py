import json
import pathlib
from typing import Collection, Dict, Iterable, Iterator, List, Optional
from urllib.error import HTTPError

import jellyfish
import orjson
import pydantic
import rtree
import shapely.geometry
import urllib3
import us
from vaccine_feed_ingest_schema import load, location

from vaccine_feed_ingest.utils.log import getLogger

from .. import vial
from ..utils import normalize
from ..utils.match import (
    is_address_similar,
    is_concordance_similar,
    is_phone_number_similar,
    is_provider_similar,
    is_provider_tag_similar,
)
from . import outputs
from .common import STAGE_OUTPUT_SUFFIX, PipelineStage

logger = getLogger(__file__)


def load_sites_to_vial(
    site_dirs: Iterable[pathlib.Path],
    output_dir: pathlib.Path,
    dry_run: bool,
    vial_server: str,
    vial_apikey: str,
    enable_match: bool,
    enable_create: bool,
    enable_rematch: bool,
    enable_reimport: bool,
    match_ids: Optional[Dict[str, str]],
    create_ids: Optional[Collection[str]],
    candidate_distance: float,
    import_batch_size: int,
) -> None:
    """Load list of sites to vial"""
    with vial.vial_client(vial_server, vial_apikey) as vial_http:
        import_run_id = vial.start_import_run(vial_http)

        locations = None
        source_summaries = None

        if enable_match or enable_create:
            logger.info("Retrieving existing locations from VIAL")
            locations = vial.retrieve_existing_locations_as_index(vial_http)
            logger.info(
                "Retrieved %d valid existing locations from VIAL", locations.get_size()
            )

        # Skip loading already matched if re-matching and re-importing
        if not enable_rematch or not enable_reimport:
            logger.info("Retrieving source locations from VIAL")
            source_summaries = vial.retrieve_source_summaries(vial_http)
            logger.info(
                "Retrieved %d valid source summaries from VIAL",
                len(source_summaries),
            )

        for site_dir in site_dirs:
            imported_locations = run_load_to_vial(
                site_dir,
                output_dir,
                dry_run=dry_run,
                vial_http=vial_http,
                import_run_id=import_run_id,
                locations=locations,
                source_summaries=source_summaries,
                enable_match=enable_match,
                enable_create=enable_create,
                enable_rematch=enable_rematch,
                enable_reimport=enable_reimport,
                match_ids=match_ids,
                create_ids=create_ids,
                candidate_distance=candidate_distance,
                import_batch_size=import_batch_size,
            )

            # If data was loaded then refresh existing locations
            if locations is not None and imported_locations:
                source_ids = [
                    loc.source_uid
                    for loc in imported_locations
                    if loc.match and loc.match.action == "new"
                ]

                if source_ids:
                    logger.info("Updating existing locations with the ones we created")
                    vial.update_existing_locations(vial_http, locations, source_ids)


def run_load_to_vial(
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    dry_run: bool,
    vial_http: urllib3.connectionpool.ConnectionPool,
    import_run_id: str,
    locations: Optional[rtree.index.Index],
    source_summaries: Optional[Dict[str, vial.SourceLocationSummary]],
    enable_match: bool = True,
    enable_create: bool = False,
    enable_rematch: bool = False,
    enable_reimport: bool = False,
    match_ids: Optional[Dict[str, str]] = None,
    create_ids: Optional[Collection[str]] = None,
    candidate_distance: float = 0.6,
    import_batch_size: int = 500,
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
    num_new_locations = 0
    num_match_locations = 0
    num_already_matched_locations = 0
    num_already_imported_locations = 0

    for filepath in outputs.iter_data_paths(
        ennrich_run_dir, suffix=STAGE_OUTPUT_SUFFIX[PipelineStage.ENRICH]
    ):
        import_locations = []
        with filepath.open(mode="rb") as src_file:
            for line in src_file:
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

                # Skip source locations that haven't changed since last load
                source_summary = None
                if source_summaries:
                    source_summary = source_summaries.get(normalized_location.id)

                    if (
                        not enable_reimport
                        and source_summary
                        and source_summary.content_hash
                    ):
                        incoming_hash = normalize.calculate_content_hash(
                            normalized_location
                        )

                        if incoming_hash == source_summary.content_hash:
                            num_already_imported_locations += 1
                            continue

                match_action = None
                if match_ids and normalized_location.id in match_ids:
                    match_action = load.ImportMatchAction(
                        action="existing",
                        id=match_ids[normalized_location.id],
                    )

                elif create_ids and normalized_location.id in create_ids:
                    match_action = load.ImportMatchAction(action="new")

                elif (enable_match or enable_create) and locations is not None:
                    # Match source locations if we are re-matching, or if we
                    # haven't matched this source location yet
                    if (
                        enable_rematch
                        or not source_summary
                        or not source_summary.matched
                    ):
                        match_action = _match_source_to_existing_locations(
                            normalized_location,
                            locations,
                            candidate_distance,
                            enable_match=enable_match,
                            enable_create=enable_create,
                        )
                    else:
                        num_already_matched_locations += 1

                import_location = _create_import_location(
                    normalized_location, match_action=match_action
                )

                num_imported_locations += 1
                if match_action:
                    if match_action.action == "existing":
                        num_match_locations += 1
                    elif match_action.action == "new":
                        num_new_locations += 1

                import_locations.append(import_location)

        if not import_locations:
            logger.warning(
                "No locations to import in %s in %s",
                filepath.name,
                site_dir.name,
            )
            continue

        if not dry_run:
            try:
                vial.import_source_locations(
                    vial_http,
                    import_run_id,
                    import_locations,
                    import_batch_size=import_batch_size,
                )
            except HTTPError as e:
                logger.warning(
                    "Failed to import some source locations for %s in %s. "
                    "Because this is action spans multiple remote calls, "
                    "some locations may have been imported: %s",
                    filepath.name,
                    site_dir.name,
                    e,
                )

            continue

    num_unknown_locations = (
        num_imported_locations
        - num_new_locations
        - num_match_locations
        - num_already_matched_locations
    )

    if enable_rematch:
        logger.info(
            "Imported %d source locations for %s "
            "(%d new, %d matched, %d unknown) and skipped %d",
            num_imported_locations,
            site_dir.name,
            num_new_locations,
            num_match_locations,
            num_unknown_locations,
            num_already_imported_locations,
        )
    else:
        logger.info(
            "Imported %d source locations for %s "
            "(%d new, %d matched, %d unknown, %d had existing match) and skipped %d",
            num_imported_locations,
            site_dir.name,
            num_new_locations,
            num_match_locations,
            num_unknown_locations,
            num_already_matched_locations,
            num_already_imported_locations,
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

    # Exclude candidates with mismatched provider tags
    provider_tag_matches = is_provider_tag_similar(source, candidate)
    if provider_tag_matches is False:
        return True

    return False


def _is_match(source: location.NormalizedLocation, candidate: dict) -> bool:
    """Return True if candidate is so similar it must be a match"""
    # If concordance matches or doesn't match then trust that first.
    concordance_matches = is_concordance_similar(source, candidate)
    if concordance_matches is not None:
        return concordance_matches

    # Don't match locations with different providers
    # Try matching with provider tag first, and then switch to provider name match
    provider_tag_matches = is_provider_tag_similar(source, candidate)

    if provider_tag_matches is not None:
        if provider_tag_matches is False:
            return False

    else:
        provider_matches = is_provider_similar(source, candidate, threshold=0.7)
        if provider_matches is not None and provider_matches is False:
            return False

    # If there are phone numbers and the phone numbers don't match then fail to match
    phone_matches = is_phone_number_similar(source, candidate)
    if phone_matches is not None and phone_matches is False:
        return False

    address_matches = is_address_similar(source, candidate)
    if address_matches is not None and address_matches is True:
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

    nearby_candidates = list(_find_candidates(source, existing, candidate_distance))

    if not nearby_candidates:
        logger.info("NEW: %s (%s): No candidates nearby", source.id, source.name)
        if enable_create:
            return load.ImportMatchAction(action="new")
        else:
            return None

    # Filter out candidates that are too different to be a match
    different_candidates = [
        loc for loc in nearby_candidates if not _is_different(source, loc)
    ]

    if not different_candidates:
        logger.info(
            "NEW: %s (%s): %d nearby candidates were not matchable",
            source.id,
            source.name,
            len(nearby_candidates),
        )
        if enable_create:
            return load.ImportMatchAction(action="new")
        else:
            return None

    # Filter to candidates that are similar enough to be the same
    candidates = [loc for loc in different_candidates if _is_match(source, loc)]

    # If there is one remaining high confidant match then use it.
    if len(candidates) == 1:
        match_candidate = candidates[0]

        logger.info(
            "MATCH: %s (%s) matched to %s (%s)",
            source.id,
            source.name,
            match_candidate["id"],
            match_candidate["properties"]["name"],
        )
        if enable_match:
            return load.ImportMatchAction(
                action="existing",
                id=match_candidate["id"],
            )
        else:
            return None

    if len(candidates) > 1:
        logger.info(
            "AMBIGUOUS: %s (%s) has %d matches e.g. %s (%s), %s (%s)",
            source.id,
            source.name,
            len(candidates),
            candidates[0]["id"],
            candidates[0]["properties"]["name"],
            candidates[1]["id"],
            candidates[1]["properties"]["name"],
        )
    else:
        logger.info(
            "MISSING: %s (%s) has no matching candidates", source.id, source.name
        )

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
        content_hash=normalize.calculate_content_hash(normalized_record),
    )

    if normalized_record.name:
        import_location.name = normalized_record.name

    if normalized_record.location:
        import_location.latitude = normalized_record.location.latitude
        import_location.longitude = normalized_record.location.longitude

    if match_action:
        import_location.match = match_action

    return import_location
