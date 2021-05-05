#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)


SOURCE_NAME = "us_giscorps_vaccine_providers"


def _get_availability(site: dict) -> schema.Availability:
    appt_only = site["attributes"]["appt_only"]

    appt_options = {
        "Yes": True,
        "No": False
    }

    avail = try_lookup(appt_options, appt_only, None, name="availability lookup")

    if avail is not None:
        return schema.Availability(appointments=avail)
    # there seems to be no walk-in data unless you want to parse "drive_in" = yes and "vehiche_required" = no into a "walk-in = yes"

    return None


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["GlobalID"]

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "c50a1a352e944a66aed98e61952051ef"
    layer = 0

    return f"{arcgis}_{layer}_{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["phone"]:
        sourcePhone = re.sub("[^0-9]", "", site["attributes"]["phone"])
        if len(sourcePhone) == 11:
            sourcePhone = sourcePhone[1:]

        # TODO: handle 3-digit phone numbers like 211, 411 .etc
        if len(sourcePhone) == 10:
            phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
            contacts.append(schema.Contact(phone=phone))

    # if site["attributes"]["publicEmail"]:
    #     contacts.append(schema.Contact(email=site["attributes"]["publicEmail"]))

    # there are multiple urls, vaccine, agency, health dept. etc
    if site["attributes"]["vaccine_url"]:
        url = site["attributes"]["vaccine_url"]
        url = sanitize_url(url)
        if url:
            contacts.append(schema.Contact(website=url))

    if len(contacts) > 0:
        return contacts

    return None


def sanitize_url(url):
    url = url.strip()
    url = url.replace("#", "")
    url = url.replace("\\", "/")  # thanks windows
    url = url if url.startswith("http") else "https://" + url
    if len(url.split(" ")) == 1:
        return url
    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    if site["attributes"]["Instructions"]:
        return [site["attributes"]["Instructions"]]

    return None


def _get_active(site: dict) -> Optional[bool]:
    # end date may be important to check to determine if the site is historicle or current but i dont really feel like digging through the docs rn. see https://github.com/CAVaccineInventory/vaccine-feed-ingest/pull/119 for links that eventually lead to specs on the
    # end_date = site["attributes"].get("end_date")

    status = site["attributes"].get("status")

    status_options = {
        "Open": True,
        "Closed": False,
        "Testing Restricted": True,
        "Scheduled to Open": False,
        "Temporarily Closed": False,
    }

    return try_lookup(status_options, status, None, name="active status lookup")


def _get_access(site: dict) -> Optional[List[str]]:
    drive = site["attributes"].get("drive_through")
    drive_bool = drive is not None

    # walk = site["attributes"].get("drive_through")
    # walk_bool = drive is not None

    wheelchair = site["attributes"].get("Wheelchair_Accessible")

    wheelchair_options = {
        "Yes": "yes",
        "Partially": "partial",
        "Unknown": "no",
        "Not Applicable": "no",
        "NA": "no",
    }
    wheelchair_bool = try_lookup(
        wheelchair_options, wheelchair, "no", name="wheelchair access"
    )

    return schema.Access(drive=drive_bool, wheelchair=wheelchair_bool)


def try_lookup(mapping, value, default, name=None):
    if value is None:
        return default

    try:
        return mapping[value]
    except KeyError as e:
        name = " for " + name or ""
        logger.error("value not present in lookup table%s: %s", name, e)

        return default


def _get_published_at(site: dict) -> Optional[str]:
    date_with_millis = site["attributes"]["CreationDate"]
    if date_with_millis:
        date = datetime.datetime.fromtimestamp(date_with_millis / 1000)  # Drop millis
        return date.isoformat()

    return None


def try_get_list(lis, index, default=None):
    if lis is None:
        return default

    try:
        value = lis[index]
        if value == "none":
            logger.warn("saw none value")
        return value
    except IndexError:
        return default


def try_get_lat_long(site):
    location = None
    try:
        location = schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        )
    except KeyError:
        pass
    
    return location


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:

    addrsplit = site["attributes"]["fulladdr"].split(", ")

    zip = site["attributes"]["fulladdr"][-5:]
    zip = zip if zip.isnumeric() else None

    city_state_zip = addrsplit[1].split(" ") if try_get_list(addrsplit, 1) else None

    state = site["attributes"]["State"] or None
    state = state.strip() if state is not None else None

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site["attributes"]["name"],
        address=schema.Address(
            street1=addrsplit[0],
            street2=None,
            city=site["attributes"]["municipality"]
            or try_get_list(city_state_zip, -3, default=""),
            state=state,
            zip=zip,
        ),
        location=try_get_lat_long(site),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,  # TODO: the format for this probably needs some mega-parsing as it looks like this -> "operhours": "Monday - Friday 8:00 am - 2:00 pm Saturdays 9:00 am - 12:00 pm",
        availability=_get_availability(site),
        inventory=None,
        access=_get_access(site),
        parent_organization=None,
        links=None,  # TODO
        notes=_get_notes(site),
        active=_get_active(site),
        source=schema.Source(
            source=SOURCE_NAME,
            id=site["attributes"]["GlobalID"],
            fetched_from_uri="https://services.arcgis.com/8ZpVMShClf8U8dae/arcgis/rest/services/Covid19_Vaccination_Locations/FeatureServer/0",  # noqa: E501
            fetched_at=timestamp,
            published_at=_get_published_at(site),
            data=site,
        ),
    )


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.ndjson")

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

for in_filepath in json_filepaths:
    filename, _ = os.path.splitext(in_filepath.name)
    out_filepath = output_dir / f"{filename}.normalized.ndjson"

    logger.info(
        "normalizing %s => %s",
        in_filepath,
        out_filepath,
    )

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for site_json in fin:
                parsed_site = json.loads(site_json)

                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
