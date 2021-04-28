#!/usr/bin/env python

import datetime
import json
import logging
import os
import pathlib
import re
import sys
from typing import List, Optional

# import schema
site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent
sys.path.append(str(root_dir))

from schema import schema  # noqa: E402

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("us/giscorps_vaccine_providers/normalize.py")


def _get_availability(site: dict) -> schema.Availability:
    appt_only = site["attributes"]["appt_only"]
    if (appt_only == "Yes"):
        return schema.Availability(appointments=True)
    elif (appt_only is not None):
        return schema.Availability(drop_in=True)

    return None


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["GlobalID"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site = "giscorps_vaccine_providers"
    runner = "us"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "c50a1a352e944a66aed98e61952051ef"
    layer = 0

    return f"{runner}:{site}:{arcgis}_{layer}:{data_id}"


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    # vaccines_field = site["attributes"]["flu_vaccinations"].lower().split(",")

    # potentials = {
    #     "pfizer": schema.Vaccine(vaccine="pfizer"),
    #     "moderna": schema.Vaccine(vaccine="moderna"),
    #     "janssen": schema.Vaccine(vaccine="janssen"),
    #     "jjj": schema.Vaccine(vaccine="janssen"),
    # }

    # inventory = []

    # for vf in vaccines_field:
    #     try:
    #         inventory.append(potentials[vf])
    #     except KeyError as e:
    #         logger.error("Unexpected vaccine type: %s", e)

    # if len(inventory) > 0:
    #     return inventory

    return None


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["phone"]:
        sourcePhone = re.sub("[^0-9]", "", site["attributes"]["phone"])
        if len(sourcePhone) == 11:
            sourcePhone = sourcePhone[1:]
        phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
        contacts.append(schema.Contact(phone=phone))

    # if site["attributes"]["publicEmail"]:
    #     contacts.append(schema.Contact(email=site["attributes"]["publicEmail"]))


    # there are multiple urls, vaccine, agency, health dept. etc    
    if site["attributes"]["vaccine_url"]:
        contacts.append(schema.Contact(website=site["attributes"]["vaccine_url"]))

    if len(contacts) > 0:
        return contacts

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
    }

    try:
        return status_options[status]
    except KeyError as e:
        logger.error("Unexpected status value: %s", e)

    return None


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
        "NA": "no"
    }
    try:
        wheelchair_bool = wheelchair_options[wheelchair]
    except KeyError:
        wheelchair_bool = "no"

    return schema.Access(drive=drive_bool, wheelchair=wheelchair_bool)


def _get_published_at(site: dict) -> Optional[str]:
    date_with_millis = site["attributes"]["CreationDate"]
    if date_with_millis:
        date = datetime.datetime.fromtimestamp(date_with_millis / 1000)  # Drop millis
        return date.isoformat()

    return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["name"],
        address=schema.Address(
            street1=site["attributes"]["fulladdr"].split(", ")[0],
            street2=None,
            city=site["attributes"]["municipality"],
            state=site["attributes"]["State"],
            zip=site["attributes"]["fulladdr"].split(" ")[-1],
        ),
        location=schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        ),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,  # TODO: the format for this probably needs some mega-parsing as it looks like this -> "operhours": "Monday - Friday 8:00 am - 2:00 pm Saturdays 9:00 am - 12:00 pm",
        availability=_get_availability(site),
        inventory=_get_inventory(site),
        access=_get_access(site),
        parent_organization=None,
        links=None,  # TODO
        notes=_get_notes(site),
        active=_get_active(site),
        source=schema.Source(
            source="arcgis",
            id=site["attributes"]["GlobalID"],
            fetched_from_uri="https://services1.arcgis.com/WzFsmainVTuD5KML/ArcGIS/rest/services/COVID19_Vaccine_Site_Survey_API/FeatureServer/0",  # noqa: E501
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
