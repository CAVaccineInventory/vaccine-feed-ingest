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
    ) -> Dict[str, Optional[str]]:
        result: Dict[str, Optional[str]] = {}

        for record_id, record in records.items():
            if not (latitude := record.get("latitude")):
                continue
            if not (longitude := record.get("longitude")):
                continue
            if not (location_name := record.get("location_name")):
                continue
            if not (street_address := record.get("street_address")):
                continue
            if not (city := record.get("city")):
                continue
            if not (region := record.get("region")):
                continue
            if not (postal_code := record.get("postal_code")):
                continue
            if not (iso_country_code := record.get("iso_country_code", "US")):
                continue

            cache_key = calculate_cache_key(
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

            cache_response = self.api_cache.get(cache_key)

            if cache_response:
                if "error" in cache_response:
                    result[record_id] = None
                    continue

                result[record_id] = cache_response.get("placekey")
                continue

            response = self._placekey_api.lookup_placekey(
                latitude=latitude,
                longitude=longitude,
                location_name=location_name,
                street_address=street_address,
                city=city,
                region=region,
                postal_code=postal_code,
                iso_country_code=iso_country_code,
                strict_address_match=strict_address_match,
                strict_name_match=strict_name_match,
            )

            if not response:
                result[record_id] = None
                continue

            if "error" in response:
                logger.info("Failed to add placekey because: %s", response["error"])
                self.set_with_expire(cache_key, {"error": response["error"]})
                result[record_id] = None
                continue

            placekey_id = response.get("placekey")

            self.set_with_expire(cache_key, {"placekey": placekey_id})

            result[record_id] = placekey_id

        return result
