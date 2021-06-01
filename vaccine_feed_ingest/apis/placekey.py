from typing import Dict, Optional

import diskcache
import placekey.api

from ..utils.log import getLogger
from .common import CachedAPI, calculate_cache_key

logger = getLogger(__file__)


class PlacekeyAPI(CachedAPI):
    """API for calling placekey that checks the cache first"""

    def __init__(self, api_cache: diskcache.Cache, apikey: str):
        self._placekey_api = placekey.api.PlacekeyAPI(apikey)
        super().__init__(api_cache)

    def lookup_placekey(
        self,
        latitude: float,
        longitude: float,
        location_name: str,
        street_address: str,
        city: str,
        region: str,
        postal_code: str,
        iso_country_code: str = "US",
        strict_address_match: bool = False,
        strict_name_match: bool = False,
    ) -> Optional[str]:
        records = {}
        records["record_0"] = {
            "latitude": latitude,
            "longitude": longitude,
            "location_name": location_name,
            "street_address": street_address,
            "city": city,
            "region": region,
            "postal_code": postal_code,
            "iso_country_code": iso_country_code,
        }

        results = self.lookup_placekeys(
            records,
            strict_address_match=strict_address_match,
            strict_name_match=strict_name_match,
        )

        return results.get("record_0")

    def lookup_placekeys(
        self,
        records: Dict[str, dict],
        strict_address_match: bool = False,
        strict_name_match: bool = False,
    ) -> Dict[str, str]:
        result: Dict[str, str] = {}

        places = []
        cache_keys = {}

        # Load valid records from cache
        for record_id, record in records.items():
            if not (latitude := record.get("latitude")):
                logger.warning("Record for placekey is missing latitutde")
                continue
            if not (longitude := record.get("longitude")):
                logger.warning("Record for placekey is missing longitude")
                continue
            if not (location_name := record.get("location_name")):
                logger.warning("Record for placekey is missing location_name")
                continue
            if not (street_address := record.get("street_address")):
                logger.warning("Record for placekey is missing street_address")
                continue
            if not (city := record.get("city")):
                logger.warning("Record for placekey is missing city")
                continue
            if not (region := record.get("region")):
                logger.warning("Record for placekey is missing region")
                continue
            if not (postal_code := record.get("postal_code")):
                logger.warning("Record for placekey is missing postal_code")
                continue
            if not (iso_country_code := record.get("iso_country_code", "US")):
                logger.warning("Record for placekey has empty iso_country_code")
                continue

            cache_keys[record_id] = calculate_cache_key(
                "placekey",
                [
                    f"{latitude:.5f}",
                    f"{longitude:.5f}",
                    location_name,
                    street_address,
                    city,
                    region,
                    postal_code,
                    iso_country_code,
                    str(strict_address_match),
                    str(strict_name_match),
                ],
            )

            cache_response = self.api_cache.get(cache_keys[record_id])

            if cache_response:
                if "error" not in cache_response and "placekey" in cache_response:
                    result[record_id] = cache_response["placekey"]
                continue

            places.append(
                {
                    "query_id": record_id,
                    "latitude": latitude,
                    "longitude": longitude,
                    "location_name": location_name,
                    "street_address": street_address,
                    "city": city,
                    "region": region,
                    "postal_code": postal_code,
                    "iso_country_code": iso_country_code,
                }
            )

        # Bulk load remaining places from API
        if not places:
            return result

        responses = self._placekey_api.lookup_placekeys(
            places,
            strict_address_match=strict_address_match,
            strict_name_match=strict_name_match,
        )

        if not responses:
            logger.info(
                "No responses from placekey bulk lookup call with %d places",
                len(places),
            )
            return result

        for response in responses:
            record_id = response.pop("query_id")
            if not record_id:
                logger.error("Placekey didn't round-trip the record id as query_id")
                continue

            if not response:
                logger.info("No data in response for record %s", record_id)
                continue

            if "error" in response:
                logger.info("Failed to add placekey because: %s", response["error"])
                self.set_with_expire(
                    cache_keys[record_id], {"error": response["error"]}
                )
                continue

            placekey_id = response.get("placekey")

            self.set_with_expire(cache_keys[record_id], {"placekey": placekey_id})

            if placekey_id:
                result[record_id] = placekey_id

        return result
