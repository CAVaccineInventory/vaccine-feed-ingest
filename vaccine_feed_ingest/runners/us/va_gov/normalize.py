#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import normalize_phone

logger = getLogger(__file__)


SOURCE_NAME = "us_giscorps_vaccine_providers"


# def _get_availability(site: dict) -> schema.Availability:
#     appt_only = site["attributes"]["appt_only"]

#     appt_options = {
#         "Yes": True,
#         "No": False,
#         "Vax only": True,
#         "Test only": False,
#     }

#     avail = try_lookup(appt_options, appt_only, None, name="availability lookup")

#     if avail is not None:
#         return schema.Availability(appointments=avail)
#     # there seems to be no walk-in data unless you want to parse "drive_in" = yes and "vehiche_required" = no into a "walk-in = yes"

#     return None


def _get_id(site: dict) -> str:
    data_id = site["id"]

    return f"{data_id}"


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["phone"]:
        for phone in normalize_phone(site["attributes"]["phone"]):
            contacts.append(phone)

    # if site["attributes"]["publicEmail"]:
    #     contacts.append(schema.Contact(email=site["attributes"]["publicEmail"]))

    # there are multiple urls, vaccine, agency, health dept. etcw
    web = site["attributes"].get("website")
    if web:
        web = sanitize_url(web)
        contacts.append(schema.Contact(website=web))

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

    notes = []
    # if site["attributes"]["operationalHoursSpecialInstructions"]:
    notes.append(site["attributes"]["operationalHoursSpecialInstructions"])

    addtl = site["attributes"].get("additionalInfo")
    if addtl:
        notes.append(addtl)
    # return None


def _get_active(site: dict) -> Optional[bool]:
    # end date may be important to check to determine if the site is historicle or current but i dont really feel like digging through the docs rn. see https://github.com/CAVaccineInventory/vaccine-feed-ingest/pull/119 for links that eventually lead to specs on the
    # end_date = site["attributes"].get("end_date")

    status = site["attributes"]["operatingStatus"].get("code")

    status_options = {"NORMAL": True, "LIMITED": True, "NOTICE": False}

    return try_lookup(status_options, status, None, name="active status lookup")


# def _get_access(site: dict) -> Optional[List[str]]:
#     drive = site["attributes"].get("drive_through")
#     drive_bool = drive is not None

#     # walk = site["attributes"].get("drive_through")
#     # walk_bool = drive is not None

#     wheelchair = site["attributes"].get("Wheelchair_Accessible")

#     wheelchair_options = {
#         "Yes": "yes",
#         "Partially": "partial",
#         "Unknown": "no",
#         "Not Applicable": "no",
#         "NA": "no",
#     }
#     wheelchair_bool = try_lookup(
#         wheelchair_options, wheelchair, "no", name="wheelchair access"
#     )

#     return schema.Access(drive=drive_bool, wheelchair=wheelchair_bool)


def try_lookup(mapping, value, default, name=None):
    if value is None:
        return default

    try:
        return mapping[value]
    except KeyError as e:
        name = " for " + name or ""
        logger.warn("value not present in lookup table%s: %s", name, e)

        return default


def _get_published_at(site: dict) -> Optional[str]:
    # date_with_millis = site["attributes"]["CreationDate"]
    # if date_with_millis:
    #     date = datetime.datetime.fromtimestamp(date_with_millis / 1000)  # Drop millis
    #     return date.isoformat()

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
            latitude=site["attributes"]["lat"],
            longitude=site["attributes"]["long"],
        )
    except KeyError:
        pass

    return location


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:

    address = site["attributes"]["address"]["physical"]

    return schema.NormalizedLocation(
        id=f"{SOURCE_NAME}:{_get_id(site)}",
        name=site["attributes"]["name"],
        address=schema.Address(
            street1=address.get("address1"),
            street2=address.get("address2"),
            city=address.get("city"),
            state=address.get("state"),
            zip=address.get("zip"),
        ),
        location=try_get_lat_long(site),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,  # TODO: the format for this probably needs some mega-parsing as it looks like this -> "operhours": "Monday - Friday 8:00 am - 2:00 pm Saturdays 9:00 am - 12:00 pm",
        availability=None,  # _get_availability(site),
        inventory=None,
        access=None,  # _get_access(site),
        parent_organization=None,
        links=None,  # TODO
        notes=_get_notes(site),
        active=_get_active(site),
        source=schema.Source(
            source=SOURCE_NAME,
            id=site["id"],
            fetched_from_uri="https://api.va.gov/v1/facilities/va?bbox%5B%5D=-180&bbox%5B%5D=-90&bbox%5B%5D=180&bbox%5B%5D=90&type=health&services%5B%5D=Covid19Vaccine&page=1&per_page=2000&radius=25000&latitude=37.408123149415275&longitude=-93.14343299172322",  # noqa: E501
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
