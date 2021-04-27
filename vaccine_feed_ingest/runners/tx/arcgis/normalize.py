#!/usr/bin/env python
import datetime
import json
import logging
import os
import pathlib
import re
import sys
from typing import List, Optional

from pydantic import ValidationError

# import schema
site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))

from schema import schema  # noqa: E402


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["OBJECTID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_source = "arcgis"
    runner = "tx"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "3078b524189848569f62985d71f4584b"
    layer = 0

    return f"{runner}:{site_source}:{arcgis}_{layer}:{data_id}"


def _get_fallback_street(address: str) -> Optional[str]:
    """
    Extract the street from the address by taking everything up to the first comma
    """
    if not address:
        return None
    match = re.search(r"(.*?),", address, flags=re.IGNORECASE)
    return match.group(1) if match else address


def _get_fallback_zip(address: str) -> Optional[str]:
    """
    Extract the ZIP from the address if the last 5 characters are digits
    """
    if not address:
        return None
    match = re.search(r"([0-9]{5})$", address, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _get_fallback_city(address) -> Optional[str]:
    """
    Attempt to extract the city from the address. First use the word immediately preceding ", TX" if present. As a
    secondary fallback, return the word immediately preceding a 5 digit zipcode
    """
    if not address:
        return None
    match = re.search(r"([\S]*),\s?TX", address, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"([\S]*)\s[0-9]{5}", address, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _get_address(site_attributes: dict) -> Optional[schema.Address]:
    address = site_attributes["ADDRESS"]
    street = (
        site_attributes["STREET"]
        if site_attributes["STREET"]
        else _get_fallback_street(address)
    )
    city = (
        site_attributes["CITY"]
        if site_attributes["CITY"]
        else _get_fallback_city(address)
    )
    zipcode = (
        site_attributes["ZIP"] if site_attributes["ZIP"] else _get_fallback_zip(address)
    )
    try:
        return schema.Address(
            street1=street,
            street2=None,
            city=city,
            state="TX",
            zip=zipcode,
        )
    except ValidationError as e:
        logging.error(
            f"Unable to parse address: "
            f"address={site_attributes['ADDRESS']}, "
            f"street={site_attributes['STREET']}, "
            f"city={site_attributes['CITY']}, "
            f"zip={site_attributes['ZIP']}, "
            f"e={e}"
        )
        return None


def _get_contacts(site_attributes: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    public_phone = site_attributes["PublicPhone"]
    if public_phone:
        # Strip whitespace, remove USA +1 or 1- and strip whitespace again
        cleaned_phone = public_phone.strip()
        cleaned_phone = (
            cleaned_phone.removeprefix("+1")
            .removeprefix("1-")
            .removeprefix("1")
            .strip()
        )
        # Match observed phone patterns. Support delimiters of dash, period, space, empty_string.
        # Allow for parentheses around area code
        match = re.match(
            r"\(*([0-9]{3})\)*[\-.\s]*([0-9]{3})[\-.\s]*([0-9]{4})", cleaned_phone
        )
        if match:
            phone = f"({match.group(1)}) {match.group(2)}-{match.group(3)}"
            contacts.append(schema.Contact(phone=phone))
        else:
            logging.warning(f"Phone Number is unexpected size or format:{public_phone}")
            contacts.append(schema.Contact(other=public_phone))

    if site_attributes["WEBSITE"]:
        contacts.append(schema.Contact(website=site_attributes["WEBSITE"]))
    return contacts if contacts else None


def _get_inventory(site_attributes: dict) -> List[schema.Vaccine]:

    normalized_vaccines = {
        "pfizer_biontech": ["PFIZER_AVAILABLE", "PFIZER_AVAILABLE2"],
        "moderna": ["MODERNA_AVAILABLE", "MODERNA_AVAILABLE2"],
        "johnson_johnson_janssen": ["JJ_AVAILABLE"],
    }
    inventory = []
    for normalized_vaccine, vaccine_keys in normalized_vaccines.items():
        supply_level = sum(
            [int(site_attributes[vaccine_key]) for vaccine_key in vaccine_keys]
        )
        inventory.append(
            schema.Vaccine(vaccine=normalized_vaccine, supply_level=supply_level)
        )
    return inventory


def _get_published_at(site_attributes: dict) -> Optional[str]:
    update_date_str = site_attributes["LAST_UPDATE_VAC"].strip()
    update_time_str = site_attributes["LAST_UPDATE_TIME_VAC"].strip()
    try:
        update_date = datetime.datetime.strptime(update_date_str, "%m/%d/%Y").date()
        update_time = datetime.datetime.strptime(update_time_str, "%H:%M:%S").time()
        combined_date = datetime.datetime.combine(update_date, update_time)
        return combined_date.isoformat()
    except ValueError as e:
        logging.error(
            f"Error parsing date update_date_str={update_date_str}, update_time_str={update_time_str}: {e}"
        )
        return None


def _get_location(site) -> Optional[schema.LatLng]:
    # A few records are missing the geometry key entirely
    if "geometry" in site:
        return schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        )
    else:
        return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    site_attributes = site["attributes"]
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site_attributes["NAME"],
        address=_get_address(site_attributes),
        location=_get_location(site),
        contact=_get_contacts(site_attributes),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=None,
        inventory=_get_inventory(site_attributes),
        access=None,
        parent_organization=None,
        links=None,
        notes=None,
        active=None,
        source=schema.Source(
            source="arcgis",
            id=site_attributes["OBJECTID"],
            fetched_from_uri="https://tdem.maps.arcgis.com/apps/webappviewer/index.html?id=3700a84845c5470cb0dc3ddace5c376b",  # noqa: E501
            fetched_at=timestamp,
            published_at=_get_published_at(site_attributes),
            data=site,
        ),
    )


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("tx/arcgis/normalize.py")


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

for in_filepath in input_dir.glob("*.ndjson"):
    filename, _ = os.path.splitext(in_filepath.name)
    out_filepath = output_dir / f"{filename}.normalized.ndjson"
    logger.info(f"normalizing {in_filepath} => {out_filepath}")

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for site_json in fin:
                parsed_site = json.loads(site_json)

                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
