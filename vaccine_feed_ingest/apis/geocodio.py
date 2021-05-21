from typing import Dict

import diskcache
import geocodio

from ..utils.log import getLogger
from .common import CachedAPI, calculate_cache_key

logger = getLogger(__file__)


class GeocodioAPI(CachedAPI):
    """API for calling geocodio that checks the cache first"""

    def __init__(self, api_cache: diskcache.Cache, apikey: str):
        self._geocodio_client = geocodio.GeocodioClient(apikey)
        super().__init__(api_cache)

    def batch_geocode(
        self,
        records: Dict[str, str],
    ) -> Dict[str, dict]:
        """Accepts full formatted addresses keyed by record id"""
        batch_result: Dict[str, dict] = {}

        addresses: Dict[str, str] = {}
        cache_keys: Dict[str, str] = {}

        # Load valid records from cache
        for record_id, address in records.items():
            if not address:
                logger.warning("Passed an empty address to geocode")
                continue

            address = address.lower()

            cache_keys[record_id] = calculate_cache_key("geocodio_geocode", [address])

            cache_response = self.api_cache.get(cache_keys[record_id])

            if cache_response:
                if "error" not in cache_response and "results" in cache_response:
                    batch_result[record_id] = cache_response["results"]
                continue

            addresses[record_id] = address

        # Bulk load remaining addresses from API
        if not addresses:
            return batch_result

        responses: Dict[str, dict] = self._geocodio_client.batch_geocode(addresses)

        if not responses:
            logger.info(
                "No response from geocodio bulk lookup call when passed %d addresses",
                len(addresses),
            )
            return batch_result

        for record_id, response in responses.items():
            if not response:
                logger.warning("Empty geocode response for %s", record_id)
                continue

            if "error" in response:
                logger.info(
                    "Failed to process address %s because: %s",
                    response.get("input", ""),
                    response["error"],
                )
                continue

            geocode_results = response.get("results")

            self.set_with_expire(cache_keys[record_id], {"results": geocode_results})

            if geocode_results:
                batch_result[record_id] = geocode_results

        return batch_result
