#!/usr/bin/env python

import datetime
import json
import pathlib
import sys
from hashlib import md5

# import schema
site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))
from schema import schema  # noqa: E402


def _generate_id(unique_str: str) -> str:
    return md5(unique_str.encode("utf-8")).hexdigest()


def normalize(site: dict, timestamp: str) -> schema.NormalizedLocation:
    # addresses are typically formatted like this: 800 Boylston Street, Boston, MA 02199
    # sometimes, "MA" is not included, so there's a handler below for that
    address = site["address"]

    address_parts = [p.strip() for p in address.split(",")]

    # Remove State + ZIP from end of Address
    state_zip = address_parts[-1]
    address_parts.pop()
    state_zip_parts = [p.strip() for p in state_zip.split(" ")]

    zipcode = state_zip_parts[1]
    city_or_state = state_zip_parts[0]

    # Sometimes, MA is not included in the address, so the first part of this is actually the city
    if city_or_state != "MA":
        city = city_or_state
    else:
        city = address_parts[-1]
        address_parts.pop()

    street1 = address_parts[0]
    street2 = None
    if len(address_parts) > 1:
        street2 = ", ".join(address_parts[1:])

    # The locations we get typically are formatted like:
    # "Abington: Walmart (Brockton Ave.)". We don't need the city name twice
    name_without_city = site["name"].split(":")[1].strip()

    location_id = _generate_id(name_without_city + address)

    return schema.NormalizedLocation(
        id=location_id,
        name=name_without_city,
        address=schema.Address(
            street1=street1,
            street2=street2,
            city=city,
            state="MA",
            zip=zipcode,
        ),
        location=None,
        contact=[
            schema.Contact(website="https://vaxfinder.mass.gov", contact_type="booking")
        ],
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=None,
        inventory=None,
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=None,
        source=schema.Source(
            source="vaxfinder",
            id=location_id,
            fetched_from_uri="https://www.mass.gov/doc/covid-19-vaccine-locations-for-currently-eligible-recipients-csv/download",  # noqa: E501
            fetched_at=timestamp,
            published_at=None,
            data=site,
        ),
    )


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

input_filepath = input_dir / "data.parsed.ndjson"

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

out_filepath = output_dir / "data.normalized.ndjson"

with input_filepath.open() as fin:
    with out_filepath.open("w") as fout:
        for site_json in fin:
            parsed_site = json.loads(site_json)

            normalized_site = normalize(parsed_site, parsed_at_timestamp)

            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
