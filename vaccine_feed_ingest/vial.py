"""Client code for calling vial"""

import contextlib
import json
import urllib.parse
from typing import Any, Dict, Iterable, Iterator, NamedTuple, Set, Tuple
from urllib.error import HTTPError

import geojson
import pydantic
import rtree
import shapely.geometry
import urllib3
from vaccine_feed_ingest_schema import load, location

from vaccine_feed_ingest.utils.log import getLogger

from .utils import misc, normalize

logger = getLogger(__file__)


@contextlib.contextmanager
def vial_client(
    server: str, apikey: str
) -> Iterator[urllib3.connectionpool.ConnectionPool]:
    """Yield a connection pool connected to vial server"""
    if not server:
        raise Exception("Must pass VIAL server to call")

    if not apikey:
        raise Exception("Must pass VIAL API Key to use")

    http_pool = urllib3.PoolManager()

    vial_http = http_pool.connection_from_url(
        server,
        pool_kwargs={"headers": {"Authorization": f"Bearer {apikey}"}},
    )

    if not verify_token(vial_http):
        raise Exception("Invalid api key for VIAL server")

    yield vial_http

    vial_http.close()


def verify_token(vial_http: urllib3.connectionpool.ConnectionPool) -> bool:
    """Verifies that header contains valid authorization token"""
    verify_resp = vial_http.request("GET", "/api/verifyToken")
    return verify_resp.status == 200


def start_import_run(vial_http: urllib3.connectionpool.ConnectionPool) -> str:
    """Start import run and return the id for it"""
    import_resp = vial_http.request("POST", "/api/startImportRun")
    if import_resp.status != 200:
        raise Exception(f"Failed to start import run {import_resp.data}")

    import_data = json.loads(import_resp.data.decode("utf-8"))
    import_run_id = import_data.get("import_run_id")

    if not import_run_id:
        raise Exception(f"Failed to start import run {import_data}")

    return import_run_id


def import_source_locations(
    vial_http: urllib3.connectionpool.ConnectionPool,
    import_run_id: str,
    import_locations: Iterable[load.ImportSourceLocation],
    import_batch_size: int = 500,
) -> None:
    """Import source locations"""
    path_and_query = f"/api/importSourceLocations?import_run_id={import_run_id}"
    logger.info("Contacting VIAL: POST %s", path_and_query)

    batches = 0
    for import_locations_batch in misc.batch(import_locations, import_batch_size):
        encoded_ndjson = "\n".join(
            [loc.json(exclude_none=True) for loc in import_locations_batch]
        )

        rsp = vial_http.request(
            "POST",
            path_and_query,
            headers={**vial_http.headers, "Content-Type": "application/x-ndjson"},
            body=encoded_ndjson.encode("utf-8"),
        )

        if rsp.status != 200:
            raise HTTPError(
                f"/api/importSourceLocations?import_run_id={import_run_id}",
                rsp.status,
                rsp.data[:100],
                dict(rsp.headers),
                None,
            )

        batches += 1
        if batches % 5 == 0:
            logger.info(
                "Submitted %d batches of up to %d records to VIAL.",
                batches,
                import_batch_size,
            )

    logger.info("Submitted %d total batches to VIAL.", batches)


def search_locations(
    vial_http: urllib3.connectionpool.ConnectionPool,
    **kwds: Any,
) -> Iterator[dict]:
    """Wrapper around search locations api. Returns geojson."""
    params = {
        **kwds,
        "format": "nlgeojson",
    }

    query = urllib.parse.urlencode(params)

    path_and_query = f"/api/searchLocations?{query}"
    logger.info("Contacting VIAL: GET %s", path_and_query)

    resp = vial_http.request("GET", path_and_query, preload_content=False)

    lines = 0
    for line in resp:
        try:
            yield geojson.loads(line)
        except json.JSONDecodeError:
            logger.warning("Invalid json record in search response: %s", line)

        lines += 1
        if lines % 5000 == 0:
            logger.info("Processed %d records from VIAL.", lines)

    logger.info("Processed %d total records from VIAL.", lines)

    resp.release_conn()


def retrieve_existing_locations(
    vial_http: urllib3.connectionpool.ConnectionPool,
) -> Iterator[dict]:
    """Return all existing locations in VIAL as geojson"""
    return search_locations(vial_http, all=1)


def _generate_index_row(loc: dict) -> Tuple[int, tuple, dict]:
    """Generate a rtree index entry from geojson entry"""
    loc_id = hash(loc["id"])
    loc_shape = shapely.geometry.shape(loc["geometry"])
    loc_bounds = loc_shape.bounds

    return (loc_id, loc_bounds, loc)


def retrieve_existing_locations_as_index(
    vial_http: urllib3.connectionpool.ConnectionPool,
) -> rtree.index.Index:
    """Return all existing locations in VIAL as rtree indexed geojson"""
    locations = retrieve_existing_locations(vial_http)
    return rtree.index.Index(_generate_index_row(loc) for loc in locations)


def update_existing_locations(
    vial_http: urllib3.connectionpool.ConnectionPool,
    locations: rtree.index.Index,
    source_ids: Iterable[str],
) -> None:
    """Updates rtree index with locations with source ids"""
    for chunked_ids in misc.batch(source_ids, 20):
        updated_locations = search_locations(vial_http, idref=list(chunked_ids))

        for loc in updated_locations:
            locations.insert(_generate_index_row(loc))


def search_source_locations(
    vial_http: urllib3.connectionpool.ConnectionPool,
    **kwds: Any,
) -> Iterator[dict]:
    """Wrapper around search source locations api. Returns geojson."""
    params = {
        **kwds,
        "format": "nlgeojson",
    }

    query = urllib.parse.urlencode(params)

    path_and_query = f"/api/searchSourceLocations?{query}"
    logger.info("Contacting VIAL: GET %s", path_and_query)

    resp = vial_http.request("GET", path_and_query, preload_content=False)

    lines = 0
    for line in resp:
        try:
            yield geojson.loads(line)
        except (json.JSONDecodeError, ValueError):
            logger.warning("Invalid json record in search response: %s", line)

        lines += 1
        if lines % 5000 == 0:
            logger.info("Processed %d records from VIAL.", lines)

    logger.info("Processed %d total records from VIAL.", lines)
    resp.release_conn()


def retrieve_matched_source_location_ids(
    vial_http: urllib3.connectionpool.ConnectionPool,
) -> Set[str]:
    """Return all matched source location ids in VIAL"""
    source_locations = search_source_locations(vial_http, all=1, matched=1)

    return {
        loc["properties"]["source_uid"]
        for loc in source_locations
        if "properties" in loc and "source_uid" in loc["properties"]
    }


class SourceLocationHash(NamedTuple):
    """Content hash and match state of source locations"""

    content_hash: str
    matched: bool


def retrieve_source_location_hashes(
    vial_http: urllib3.connectionpool.ConnectionPool,
) -> Dict[str, SourceLocationHash]:
    """Return content hash and match state of source locations keyed by source uid"""
    source_locations = search_source_locations(vial_http, all=1)

    results = {}

    for loc in source_locations:
        if "properties" not in loc:
            continue

        source_uid = loc["properties"].get("source_uid")
        if not source_uid:
            continue

        import_json = loc["properties"].get("import_json")
        if not import_json:
            continue

        try:
            imported_location = location.NormalizedLocation.parse_obj(import_json)
        except pydantic.ValidationError as e:
            logger.warning(
                "Ignoring existing source location because it is invalid: %s\n%s",
                source_uid,
                str(e),
            )
            continue

        content_hash = normalize.calculate_content_hash(imported_location)

        matched = False
        matched_location = loc["properties"].get("matched_location")
        if matched_location and matched_location.get("id"):
            matched = True

        results[source_uid] = SourceLocationHash(
            content_hash=content_hash, matched=matched
        )

    return results
