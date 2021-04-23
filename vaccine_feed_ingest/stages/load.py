import logging
import pathlib
from typing import Callable, Optional, List, Iterable, Iterator

import geopandas as gpd
import shapely
import pydantic
import urllib3
from vaccine_feed_ingest import vial
from vaccine_feed_ingest.schema import schema

from . import outputs
from .common import PipelineStage

logger = logging.getLogger("load")


# Collect locations that are within .1 degrees = 11.1 km = 6.9 mi
CANDIDATE_DEGREES_DISTANCE = 0.1

# Disabling create new for now
ENABLE_CREATE_NEW = False


def run_load_to_vial(
    vial_http: urllib3.connectionpool.ConnectionPool,
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    import_run_id: str,
    locations: Optional[List[dict]],
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
                if locations is not None:
                    match_action = _match_source_to_existing_locations(
                        normalized_location, locations
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
    existing: Iterable[dict],
) -> Iterator[dict]:
    """Return a slice of existing locations"""
    src_point = shapely.geometry.Point(
        source.location.latitude,
        source.location.longitude,
    )

    for loc in existing:
        if "geometry" not in loc:
            continue

        loc_point = shapely.geometry.shape(loc["geometry"])

        if loc_point.distance(src_point) < CANDIDATE_DEGREES_DISTANCE:
            yield loc


def _is_different(source: schema.NormalizedLocation) -> Callable[[gpd.GeoSeries], bool]:
    """Return True if candidate is so different it couldn't be a match"""

    def _fn(candidate: dict) -> bool:
        return False

    return _fn


def _is_match(source: schema.NormalizedLocation) -> Callable[[gpd.GeoSeries], bool]:
    """Return True if candidate is so similar it must be a match"""

    def _fn(candidate: dict) -> bool:
        return False

    return _fn


def _match_source_to_existing_locations(
    source: schema.NormalizedLocation,
    existing: Iterable[dict],
) -> Optional[schema.ImportMatchAction]:
    """Attempt to match source location to existing locations"""
    if not source.location:
        return None

    candidates = list(_find_candidates(source, existing))

    if not candidates:
        if ENABLE_CREATE_NEW:
            return schema.ImportMatchAction(action="new")
        else:
            return None

    # Filter out candidates that are too different to be a match
    candidates = [loc for loc in candidates if not _is_different(source)]

    if not candidates:
        if ENABLE_CREATE_NEW:
            return schema.ImportMatchAction(action="new")
        else:
            return None

    # Filter to candidates that are similar enough to be the same
    candidates = [loc for loc in candidates if _is_match(source)]

    # If there is one remaining high confidant match then use it.
    if len(candidates) == 1:
        return schema.ImportMatchAction(
            action="match",
            id=candidates.iloc[0]["id"],
        )

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
