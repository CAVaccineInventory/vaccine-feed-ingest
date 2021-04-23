"""Client code for calling vial"""

import contextlib
import json
import geojson
from typing import Iterable, Iterator

import urllib3
from vaccine_feed_ingest.schema import schema


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
    encoded_ndjson = "\n".join([loc.json() for loc in import_locations])

    return vial_http.request(
        "POST",
        f"/api/importSourceLocations?import_run_id={import_run_id}",
        headers={**vial_http.headers, "Content-Type": "application/x-ndjson"},
        body=encoded_ndjson.encode("utf-8"),
    )


def retrieve_existing_locations(
    vial_http: urllib3.connectionpool.ConnectionPool,
) -> Iterator[dict]:
    """Verifies that header contains valid authorization token"""
    resp = vial_http.request(
        "GET", "/api/searchLocations?format=nlgeojson&all=1", preload_content=False
    )

    for line in resp:
        yield geojson.loads(line)

    resp.release_conn()
