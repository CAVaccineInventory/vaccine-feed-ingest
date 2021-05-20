#!/usr/bin/env python

import datetime
import json
import pathlib
import sys
from typing import Dict, List, Optional, OrderedDict, Tuple

import usaddress
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

Site = Dict[str, str]


def _get_site_id_parts(site: Site) -> Tuple[str, str]:
    """Returns a tuple consisting of the global label 'us_physicians_immediate' followed by a generated
    identifier for an individual site. Callers can decide to join the pieces with a colon to make a normalized
    location id or to just look at the right side of the tuple to find the source location ID.
    """
    return ("us_physicians_immediate", f"{site['table_id']}_{site['row_id']}")


def get_normalized_location_id(site: Site) -> str:
    return ":".join(_get_site_id_parts(site))


def get_source_location_id(site: Site) -> str:
    return _get_site_id_parts(site)[1]


def infer_address(site: Site) -> schema.Address:
    """The source data does not separate address components into separate fields.
    This function imports the probabilistic parser from `usaddress` to make sense
    of the scraped address data. Some extra patches are applied to fix errors in this
    specfic dataset.
    """

    parsed_address, address_type = usaddress.tag(site["address"])
    if address_type != "Street Address":
        raise NotImplementedError(
            f"Address normalization is only implemented for 'Street Address', not {address_type}"
        )
    patched_address = apply_address_fixups(parsed_address)

    # Building up kwargs lets us easily omit fields for which we have no data.
    # In particular, many sites don't provide a zip code
    address_kwargs = {
        # "street1",
        # "city",
        # "state",
        # "zip"
    }
    street_buffer: List[str] = []
    while len(patched_address) > 0:
        component, value = patched_address.popitem(last=False)
        if component == "PlaceName":
            address_kwargs["city"] = value
        elif component == "StateName":
            address_kwargs["state"] = value
        elif component == "ZipCode":
            address_kwargs["zip"] = value
        else:
            street_buffer.append(value)
    address_kwargs["street1"] = " ".join(street_buffer)

    return schema.Address(**address_kwargs)


def apply_address_fixups(address: OrderedDict[str, str]) -> OrderedDict[str, str]:
    """Sometimes the usaddress parser makes mistakes. It's an imperfect world.
    This function applies transformations to the parsed address to correct specific
    known errors.
    """
    # Fixup: At least one address has "WI, USA" in the "StateName" component.
    # Strip non-state components
    address["StateName"] = address["StateName"].partition(",")[0]

    # Fixup: (OrderedDict([('AddressNumber', '1019'),
    #           ('StreetNamePreDirectional', 'S.'),
    #           ('StreetName', 'Green Bay Road'),
    #           ('StreetNamePostType', 'Mount'),
    #           ('PlaceName', 'Pleasant'),
    #           ('StateName', 'WI'),
    #           ('ZipCode', '53406')]),
    #        'Street Address'),
    #
    # The correct name of the town is "Mount Pleasant"
    if (
        address.get("StreetNamePostType") == "Mount"
        and address.get("PlaceName") == "Pleasant"
    ):
        del address["StreetNamePostType"]
        del address["PlaceName"]
        address["PlaceName"] = "Mount Pleasant"

    # Fixup:
    #  (OrderedDict([('AddressNumber', '3111'),
    #                ('StreetNamePreDirectional', 'S.'),
    #                ('PlaceName', 'Chicago South Milwaukee'),
    #                ('StateName', 'WI'),
    #                ('ZipCode', '53172')]),
    #  'Street Address'),
    #
    # 'Chicago' is a 'StreetName', located in the town of 'South Milwaukee'
    if address.get("PlaceName") == "Chicago South Milwaukee":
        del address["PlaceName"]
        address["StreetName"] = "Chicago"
        address["PlaceName"] = "South Milwaukee"

    # Fixup:
    # (OrderedDict([('AddressNumber', '2490'),
    #               ('StreetName', 'Bushwood'),
    #               ('PlaceName', 'Dr.Elgin'),
    #               ('StateName', 'IL')]),
    # 'Street Address'),
    #
    # 'Dr.Elgin' is a typographical error. Create a 'StreetNamePostType' of 'Dr.' and a new 'PlaceName' of 'Elgin'
    if address.get("PlaceName") == "Dr.Elgin":
        del address["PlaceName"]
        address["StreetNamePostType"] = "Dr."
        address["PlaceName"] = "Elgin"

    return address


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
        "johnson & johnson": schema.VaccineType.JOHNSON_JOHNSON_JANSSEN,
    }
    match = potentials.get(site["type"].lower().strip())

    if match is None:
        return None
    else:
        return [{"vaccine": match, "supply_level": supply}]


def normalize(site: Site, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=get_normalized_location_id(site),
        name=site["clinic"],
        address=infer_address(site),
        inventory=parse_vaccine(site),
        source=schema.Source(
            source="us_physicians_immediate",
            id=get_source_location_id(site),
            fetched_from_uri="https://physiciansimmediatecare.com/covid-19-vaccination-locations/",
            fetched_at=timestamp,
            data=site,
        ),
    )


if __name__ == "__main__":
    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])
    processing_timestamp = datetime.datetime.utcnow().isoformat()

    for ingest_path in input_dir.glob("*.ndjson"):
        output_path = output_dir / (ingest_path.with_suffix(".normalized.ndjson").name)

        with ingest_path.open("r") as ingest_file:
            with output_path.open("w") as output_file:
                for site_json in ingest_file:
                    parsed_site = json.loads(site_json)

                    try:
                        normalized_site = normalize(parsed_site, processing_timestamp)
                        json.dump(normalized_site.dict(), output_file)
                        output_file.write("\n")
                    except Exception as e:
                        logger.warn(
                            f"Failed to normalize site. Error: {e}\nsite JSON: {site_json}"
                        )
