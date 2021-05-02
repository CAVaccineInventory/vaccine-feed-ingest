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


def _get_availability(site: dict) -> schema.Availability:
    avail_field = site["attributes"]["flu_walkins"]

    potentials = {
        "no_please_make_an_appointment": schema.Availability(appointments=True),
        "yes": schema.Availability(drop_in=True),
    }

    try:
        return potentials[avail_field]
    except KeyError as e:
        logger.error("Unexpected availability code: %s", e)

    return None


def _get_id(site: dict) -> str:
    data_id = site["attributes"]["globalid"]

    # Could parse these from directory traversal, but do not for now to avoid
    # accidental mutation.
    site_name = "arcgis"
    runner = "ak"

    # Could parse these from the input file name, but do not for now to avoid
    # accidental mutation.
    arcgis = "d92cbd6ff2524d7e92bef109f30cb366"
    layer = 0

    return f"{runner}_{site_name}:{arcgis}_{layer}_{data_id}"


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    vaccines_field = site["attributes"]["flu_vaccinations"].lower().split(",")

    potentials = {
        "pfizer": schema.Vaccine(vaccine="pfizer_biontech"),
        "moderna": schema.Vaccine(vaccine="moderna"),
        "janssen": schema.Vaccine(vaccine="johnson_johnson_janssen"),
        "jjj": schema.Vaccine(vaccine="johnson_johnson_janssen"),
    }

    inventory = []

    for vf in vaccines_field:
        try:
            inventory.append(potentials[vf])
        except KeyError as e:
            logger.error("Unexpected vaccine type: %s", e)

    if len(inventory) > 0:
        return inventory

    return None


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []
    if site["attributes"]["phone"]:
        sourcePhone = re.sub("[^0-9]", "", site["attributes"]["phone"])
        if len(sourcePhone) == 11:
            sourcePhone = sourcePhone[1:]
        phone = f"({sourcePhone[0:3]}) {sourcePhone[3:6]}-{sourcePhone[6:]}"
        contacts.append(schema.Contact(phone=phone))

    if site["attributes"]["publicEmail"]:
        contacts.append(schema.Contact(email=site["attributes"]["publicEmail"]))

    if site["attributes"]["publicWebsite"]:
        contacts.append(schema.Contact(website=site["attributes"]["publicWebsite"]))

    if len(contacts) > 0:
        return contacts

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    if site["attributes"]["publicNotes"]:
        return [site["attributes"]["publicNotes"]]

    return None


def _get_published_at(site: dict) -> Optional[str]:
    date_with_millis = site["attributes"]["datesubmited"]
    if date_with_millis:
        date = datetime.datetime.fromtimestamp(date_with_millis / 1000)  # Drop millis
        return date.isoformat()

    return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=site["attributes"]["vaccinationSite"],
        address=schema.Address(
            street1=site["attributes"]["address"],
            street2=None,
            city=site["attributes"]["city"],
            state="AK",
            zip=site["attributes"]["zipcode"],
        ),
        location=schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        ),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=_get_availability(site),
        inventory=_get_inventory(site),
        access=None,
        parent_organization=None,
        links=None,
        notes=_get_notes(site),
        active=None,
        source=schema.Source(
            source="ak_arcgis",
            id=site["attributes"]["globalid"],
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
