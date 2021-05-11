#!/usr/bin/env python

import datetime
import json
import pathlib
import sys
import re
from typing import List, Optional, Dict

from pydantic.error_wrappers import ValidationError

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

Site = Dict[str, str]


def get_site_id(site: Site) -> str:
    return f"us_physicians_immediate:{hash(site['address'])}"


def infer_address(site: Site) -> schema.Address:
    """The source data does not separate address components into separate fields.
    This function makes some guesses about how to extract data from these unstructured
    addresses, when available.

    NB: The entire source address is dumped into the 'street1' field. If these heuristics
    work then additional fields will be filled out, but we don't remove any information
    from the original address string
    """

    def maybe_last_match(pattern: re.Pattern, text: str) -> Optional[str]:
        matches = re.findall(pattern, text)
        if matches is not None and len(matches) > 0:
            return matches[-1]
        else:
            return None

    # Look for a two-letter state postal abbreviation. If there are multiple matches,
    # the state is more likely to be at the end of the string
    state_re = re.compile(r"\b([A-Z]{2})\b")

    # Look for a ZIP code. Insist that it be at the end of the string, so that
    # we don't match "19165 West Bluemound Road, Brookfield, WI, USA"
    zip_re = re.compile(r"\b([0-9]{5}(?:-[0-9]{4})?)\b\s*$")

    return schema.Address(
        state=maybe_last_match(state_re, site['address']),
        street1=site['address'],
        zip=maybe_last_match(zip_re, site['address'])
    )


def parse_vaccine(site: Site) -> Optional[List[schema.Vaccine]]:
    """Not all sites report what type of vaccine they carry, or report "-" when
    they are out of supply. Every site reports at most one type of vaccine
    """
    # Bail early on sites with no vaccine info
    if site.get("type") is None:
        return None

    if int(site["slots"]) > 0:
        supply = schema.VaccineSupply.IN_STOCK
    else:
        supply = schema.VaccineSupply.OUT_OF_STOCK

    potentials = {
        "pfizer": schema.VaccineType.PFIZER_BIONTECH,  # not observed in the data
        "moderna": schema.VaccineType.MODERNA,
        "johnson & johnson": schema.VaccineType.JOHNSON_JOHNSON_JANSSEN
    }
    match = potentials.get(site["type"].lower().strip())

    if match is None:
        return None
    else:
        return [{
            "vaccine": match,
            "supply_level": supply
            }]


def normalize(site: Site, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=get_site_id(site),
        name=site['clinic'],
        address=infer_address(site),
        inventory=parse_vaccine(site),
        source=schema.Source(
            source="us_physicians_immediate",
            id=get_site_id(site).split(':')[-1],
            fetched_from_uri="https://physiciansimmediatecare.com/covid-19-vaccination-locations/",
            fetched_at=timestamp,
            data=site
        )
    )


if __name__ == "__main__":
    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])
    processing_timestamp = datetime.datetime.utcnow().isoformat()

    for ingest_path in input_dir.glob("*.ndjson"):
        output_path = output_dir / (ingest_path.with_suffix(".normalized.ndjson").name)

        logger.info("Reading site data from %s", ingest_path.name)
        with ingest_path.open("r") as ingest_file:
            with output_path.open("w") as output_file:
                for site_json in ingest_file:
                    parsed_site = json.loads(site_json)

                    normalized_site = normalize(parsed_site, processing_timestamp)

                    json.dump(normalized_site.dict(), output_file)
                    output_file.write("\n")
