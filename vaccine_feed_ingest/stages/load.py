import logging
import pathlib

import pydantic
import urllib3
from vaccine_feed_ingest import vial
from vaccine_feed_ingest.schema import schema

from . import outputs
from .common import PipelineStage

logger = logging.getLogger("load")


def run_load_to_vial(
    vial_http: urllib3.connectionpool.ConnectionPool,
    site_dir: pathlib.Path,
    output_dir: pathlib.Path,
    import_run_id: str,
) -> bool:
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

                import_locations.append(_create_import_location(normalized_location))

        if not import_locations:
            logger.warning(
                "No locations to import in %s in %s",
                filepath.name,
                site_dir.name,
            )
            continue

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


def _create_import_location(
    normalized_record: schema.NormalizedLocation,
) -> schema.ImportSourceLocation:
    """Transform normalized record into import record"""
    import_location = schema.ImportSourceLocation(
        source_uid=normalized_record.id,
        source_name=normalized_record.source.source,
        import_json=normalized_record,
        # TODO: Add code to match to existing entities
        match=schema.ImportMatchAction(action="new"),
    )

    if normalized_record.name:
        import_location.name = normalized_record.name

    if normalized_record.location:
        import_location.latitude = normalized_record.location.latitude
        import_location.longitude = normalized_record.location.longitude

    return import_location
