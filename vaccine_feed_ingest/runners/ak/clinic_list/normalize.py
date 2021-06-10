#!/usr/bin/env python

import datetime
import os
import pathlib
import sys
from typing import List, Optional

import orjson
from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import normalize_phone, normalize_url

SITE_NAME = "clinic_list"
RUNNER = "ak"


logger = getLogger(__file__)


def _get_id(site: dict) -> str:
    return site["attributes"]["globalid"]


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    ret = []
    if phone := site["attributes"]["phone"]:
        ret.extend(normalize_phone(phone))
    if email := site["attributes"]["publicEmail"]:
        ret.append(schema.Contact(email=email))
    if website := site["attributes"]["publicWebsite"]:
        ret.append(schema.Contact(website=normalize_url(website)))
    return ret


def _get_availability(site: dict) -> Optional[schema.Availability]:
    if site["attributes"]["flu_walkins"] == "no_please_make_an_appointment":
        return schema.Availability(drop_in=False)
    return None


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    vaccine_names = [
        ("pfizer", schema.VaccineType.PFIZER_BIONTECH),
        ("moderna", schema.VaccineType.MODERNA),
        ("jjj", schema.VaccineType.JOHNSON_JOHNSON_JANSSEN),
    ]

    ret = []
    for their_name, our_name in vaccine_names:
        if their_name in site["attributes"]["flu_vaccinations"]:
            ret.append(schema.Vaccine(vaccine=our_name))

    return ret


def _get_organization(site: dict) -> Optional[schema.Organization]:
    emails = [site["attributes"]["publicEmail"], site["attributes"]["adminEmail"]]

    for email in emails:
        if email and "costco.com" in email:
            return schema.Organization(id=schema.VaccineProvider.COSTCO)
        if email and "cvshealth.com" in email:
            return schema.Organization(id=schema.VaccineProvider.CVS)
        if email and "fredmeyer.com" in email:
            return schema.Organization(id=schema.VaccineProvider.FRED_MEYER)
        if email and "safeway.com" in email:
            return schema.Organization(id=schema.VaccineProvider.SAFEWAY)
        if email and "walgreens.com" in email:
            return schema.Organization(id=schema.VaccineProvider.WALGREENS)
        if email and "walmart.com" in email:
            return schema.Organization(id=schema.VaccineProvider.WALMART)
    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    if notes := site["attributes"]["publicNotes"]:
        return [notes]
    return None


def _get_normalized_location(
    site: dict, timestamp: str, filename: str
) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=f"{RUNNER}_{SITE_NAME}:{_get_id(site)}",
        name=site["attributes"]["vaccinationSite"],
        address=schema.Address(
            street1=site["attributes"]["address"],
            street2=None,
            city=site["attributes"]["city"],
            state=schema.State.ALASKA,
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
        parent_organization=_get_organization(site),
        links=None,
        notes=_get_notes(site),
        active=None,
        source=schema.Source(
            source=f"{RUNNER}_{SITE_NAME}",
            id=_get_id(site),
            fetched_from_uri="https://anchoragecovidvaccine.org/providers/",
            fetched_at=timestamp,
            published_at=site["attributes"]["EditDate"],
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

    with in_filepath.open("rb") as fin:
        with out_filepath.open("wb") as fout:
            # Optimization: faster to batch all of the sites into a single
            # write than to do ~46k separate writes.
            # Optimization: using orjson rather than json.
            fout.write(
                b"\n".join(
                    orjson.dumps(
                        _get_normalized_location(
                            orjson.loads(site_json),
                            parsed_at_timestamp,
                            filename,
                        ).dict()
                    )
                    for site_json in fin
                )
            )
            fout.write(b"\n")
