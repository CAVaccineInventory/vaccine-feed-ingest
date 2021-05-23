#!/usr/bin/env python3
import datetime
import json
import pathlib
import sys
from typing import List, Optional

from pydantic import ValidationError
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)
SOURCE_NAME = "wa_state"


def _get_id(site: dict) -> str:
    # The graphql docs say this id should be unique to this location within
    # this data source
    locationId = site["locationId"]

    return locationId


def _get_contacts(site: dict) -> List[schema.Contact]:
    # From the docs for this data source:
    # These are nullable fields, but one is "guaranteed" to be non nullable
    phone, email, scheduling_link = (
        site.get("phone"),
        site.get("email"),
        site.get("schedulingLink"),
    )

    info_link = site.get("infoLink")
    try:
        phone_contact = (
            schema.Contact(
                contact_type="booking",
                phone=phone,
            )
            if phone is not None
            else None
        )
    except ValidationError:
        logger.warning(f"Invalid phone contact {phone}")
        phone_contact = None

    try:
        email_contact = (
            schema.Contact(
                contact_type="booking",
                email=email,
            )
            if email is not None
            else None
        )
    except ValidationError:
        logger.warning(f"Invalid email contact {email}")
        email_contact = None

    try:
        website_contact = (
            schema.Contact(
                contact_type="booking",
                website=scheduling_link,
            )
            if scheduling_link is not None
            else None
        )
    except ValidationError:
        logger.warning(f"Invalid scheduling_link contact {scheduling_link}")
        website_contact = None

    try:
        general_contact = (
            schema.Contact(contact_type="general", website=info_link)
            if info_link
            else None
        )
    except ValidationError:
        logger.warning(f"Invalid info_link contact {info_link}")
        general_contact = None

    return [
        contact
        for contact in (phone_contact, email_contact, website_contact, general_contact)
        if contact is not None
    ]


def _get_wheelchair(site):
    if site.get("wheelchairAccessible") is True:
        return "yes"
    if site.get("wheelchairAccessible") is False:
        return "no"

    return None


def _get_supply_level(site):
    if site["vaccineAvailability"] == "AVAILABLE":
        return "in_stock"

    if site["vaccineAvailability"] == "UNAVAILABLE":
        return "out_of_stock"

    # Docs say the value should be UNKNOWN
    return None


def _get_vaccine_type(type_string):
    if type_string == "johnsonAndJohnson":
        return "johnson_johnson_janssen"
    if type_string == "moderna":
        return "moderna"
    if type_string == "pfizer":
        return "pfizer_biontech"
    return None


def _get_good_zip(site: dict) -> Optional[str]:
    zip = site["zipcode"]
    if len(zip) != 5:
        if zip[0:2] == "PR" and len(zip) == 7:
            return zip[2:]

        logger.warning(
            "%s:%s has invalid zip value %s. Using None",
            SOURCE_NAME,
            site["locationId"],
            zip,
        )
        return None

    return zip


def _get_state(site: dict) -> Optional[str]:
    state_long_name = site.get("state")
    if not state_long_name:
        logger.warning(
            "%s:%s has no state in the parsed data", SOURCE_NAME, site["locationId"]
        )
        return None

    if state_long_name == "Hawai'i":
        state_long_name = "HAWAII"

    try:
        return schema.State[state_long_name.strip().upper().replace(" ", "_")].value
    except KeyError:
        logger.warning(
            "%s:%s state '%s' could not be parsed to a valid enum. Using %s.",
            SOURCE_NAME,
            site["locationId"],
            state_long_name,
            state_long_name,
        )
        return state_long_name


def normalize(site: dict, timestamp: str) -> schema.NormalizedLocation:
    source_name = SOURCE_NAME

    # NOTE: we use `get` where the field is optional in our data source, and
    # ["key'] access where it is not.
    return schema.NormalizedLocation(
        id=f"{source_name}:{_get_id(site)}",
        name=site["locationName"],
        address=schema.Address(
            street1=site.get("addressLine1"),
            street2=site.get("addressLine2"),
            city=site.get("city"),
            state=_get_state(site),
            zip=_get_good_zip(site),
        ),
        location=schema.LatLng(latitude=site["latitude"], longitude=site["longitude"]),
        contact=_get_contacts(site),
        notes=site.get("description"),
        # Since this could be nullable we make sure to only provide it if it's True or False
        availability=schema.Availability(drop_in=site.get("walkIn"))
        if site.get("walkIn") is not None
        else None,
        access=schema.Access(
            walk=site.get("walkupSite"),
            drive=site.get("driveupSite"),
            wheelchair=_get_wheelchair(site),
        ),
        # IF supply_level is UNKNOWN, don't say anything about it
        inventory=[
            schema.Vaccine(
                vaccine=_get_vaccine_type(vaccine), supply_level=_get_supply_level(site)
            )
            for vaccine in site["vaccineTypes"]
            if _get_vaccine_type(vaccine) is not None
        ]
        if _get_supply_level(site)
        else None,
        parent_organization=schema.Organization(
            id=site.get("providerId"), name=site.get("providerName")
        ),
        source=schema.Source(
            source=source_name,
            id=site["locationId"],
            fetched_from_uri="https://apim-vaccs-prod.azure-api.net/web/graphql",
            fetched_at=timestamp,
            published_at=site["updatedAt"],
            data=site,
        ),
    )


parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

input_dir = pathlib.Path(sys.argv[2])
input_file = input_dir / "data.parsed.ndjson"
output_dir = pathlib.Path(sys.argv[1])
output_file = output_dir / "data.normalized.ndjson"

with input_file.open() as parsed_lines:
    with output_file.open("w") as fout:
        for line in parsed_lines:
            site_blob = json.loads(line)

            normalized_site = normalize(site_blob, parsed_at_timestamp)

            json.dump(normalized_site.dict(), fout)
            fout.write("\n")
