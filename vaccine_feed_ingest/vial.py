"""Client code for calling vial"""

import contextlib
import json
import logging
import urllib.parse
from typing import Any, Iterable, Iterator, Tuple

import geojson
import rtree
import shapely.geometry
import urllib3
from vaccine_feed_ingest.schema import schema
from vaccine_feed_ingest.utils import misc

logger = logging.getLogger("vial")


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
    import_locations: Iterable[schema.ImportSourceLocation],
) -> urllib3.response.HTTPResponse:
    """Import source locations"""
    for import_locations_batch in misc.batch(import_locations, 1_000):
        encoded_ndjson = "\n".join(
            [loc.json(exclude_none=True) for loc in import_locations_batch]
        )

        return vial_http.request(
            "POST",
            f"/api/importSourceLocations?import_run_id={import_run_id}",
            headers={**vial_http.headers, "Content-Type": "application/x-ndjson"},
            body=encoded_ndjson.encode("utf-8"),
        )


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

    resp = vial_http.request(
        "GET", f"/api/searchLocations?{query}", preload_content=False
    )

    for line in resp:
        try:
            yield geojson.loads(line)
        except json.JSONDecodeError:
            logger.warning("Invalid json record in search response: %s", line)

    resp.release_conn()


def retrieve_existing_locations(
    vial_http: urllib3.connectionpool.ConnectionPool,
) -> Iterator[dict]:
    """Return all existing locations in VIAL as geojson"""
    return search_locations(vial_http, all=1)


def _generate_index_row(loc: dict) -> Tuple[int, tuple, dict]:
    """Generate a rtree index entry from geojson entry"""
    loc_id = hash(loc["properties"]["id"])
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
