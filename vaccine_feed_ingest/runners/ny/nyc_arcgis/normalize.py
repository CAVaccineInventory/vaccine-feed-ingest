#!/usr/bin/env python
# isort: skip_file

import datetime
import json
from vaccine_feed_ingest.utils.log import getLogger
import os
import pathlib
import re
import sys
from typing import List, Optional

import pytz

from vaccine_feed_ingest.schema import schema  # noqa: E402


logger = getLogger(__file__)

utc_tz = pytz.timezone("UTC")


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    if site["attributes"]["Phone"] and re.match(
        r"^\(\d{3}\) \d{3}-\d{4}$", site["attributes"]["Phone"]
    ):
        contacts.append(
            schema.Contact(contact_type="general", phone=site["attributes"]["Phone"])
        )

    if site["attributes"]["Website"]:
        contacts.append(
            schema.Contact(
                contact_type="general", website=site["attributes"]["Website"]
            )
        )

    if len(contacts) > 0:
        return contacts

    return None


def _get_opening_hours(site: dict) -> Optional[List[schema.OpenHour]]:
    weekdays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    hours = []
    for day in weekdays:
        open_time = site["attributes"][f"HoursStart_{day}"]
        close_time = site["attributes"][f"ClosingHour_{day}"]
        if open_time != -1 and close_time != -1:
            open_time = f"{open_time:02}"
            close_time = f"{close_time:02}"
            hours.append(
                schema.OpenHour(
                    day=day.lower(),
                    open=f"{open_time[0:2]}:{open_time[2:4]}",
                    closes=f"{close_time[0:2]}:{close_time[2:4]}",
                )
            )

    if len(hours) > 0:
        return hours

    return None


def _get_availability(site: dict) -> schema.Availability:
    appointment_required = site["attributes"]["Intake_ApptRequired"] == "Yes"
    drop_ins_allowed = site["attributes"]["Intake_WalkIn"] == "Yes"

    return schema.Availability(
        drop_in=(
            True if drop_ins_allowed else (False if appointment_required else None)
        ),
        appointments=(True if appointment_required else None),
    )


def _get_inventory(site: dict) -> List[schema.Vaccine]:
    vaccines = []

    if site["attributes"]["ServiceType_JohnsonAndJohnson"] == "Yes":
        vaccines.append(schema.Vaccine(vaccine="johnson_johnson_janssen"))

    if site["attributes"]["ServiceType_Moderna"] == "Yes":
        vaccines.append(schema.Vaccine(vaccine="moderna"))

    if site["attributes"]["ServiceType_Pfizer"] == "Yes":
        vaccines.append(schema.Vaccine(vaccine="pfizer_biontech"))

    return vaccines


def _get_parent_organization(site: dict) -> Optional[schema.Organization]:
    # Just the obvious easy cases; Pareto-style
    name = site["attributes"]["FacilityName"]
    if name == "Rite Aid Pharmacy":
        return schema.Organization(name="rite_aid")
    if name == "Walgreens/Duane Reade":
        return schema.Organization(name="walgreens")
    if name == "CVS Pharmacy":
        return schema.Organization(name="cvs")

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    note = site["attributes"]["AdditionalInfo"]
    if note:
        return [note]

    return None


def _get_source(site: dict, timestamp: str) -> schema.Source:
    source_uri = (
        "https://services1.arcgis.com/oOUgp466Coyjcu6V/ArcGIS/rest/services"
        + "/VaccineFinder_Production_View/FeatureServer/0"
    )
    return schema.Source(
        source="nyc_arcgis",
        id=site["attributes"]["LocationID"],
        fetched_from_uri=source_uri,
        fetched_at=timestamp,
        published_at=datetime.datetime.fromtimestamp(
            site["attributes"]["LastModifiedDate"] / 1000, utc_tz
        ).isoformat(),
        data=site,
    )


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=f"nyc_arcgis:{site['attributes']['LocationID']}",
        name=site["attributes"]["FacilityName"],
        address=schema.Address(
            street1=site["attributes"]["Address"],
            street2=site["attributes"]["Address2"],
            city=site["attributes"]["Borough"],
            state="NY",
            zip=site["attributes"]["Zipcode"],
        ),
        location=schema.LatLng(
            latitude=site["geometry"]["y"],
            longitude=site["geometry"]["x"],
        ),
        contact=_get_contacts(site),
        opening_hours=_get_opening_hours(site),
        availability=_get_availability(site),
        inventory=_get_inventory(site),
        access=schema.Access(
            wheelchair="yes" if site["attributes"]["ADA_Compliant"] == "Yes" else "no"
        ),
        parent_organization=_get_parent_organization(site),
        notes=_get_notes(site),
        source=_get_source(site, timestamp),
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
