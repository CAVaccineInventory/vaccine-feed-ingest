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
from vaccine_feed_ingest.utils.normalize import provider_id_from_name
from vaccine_feed_ingest.utils.parse import location_id_from_name

logger = getLogger(__file__)


def _get_address(site: dict) -> schema.Address:
    city = None
    zip = None

    m = re.match(r"(.+),?\s+(CA[,.]?\s*)?(?:(\d{5})[,-]?)?", site["addr2"])
    if m:
        city = m.group(1)
        zip = m.group(3)

    # A few entries have the whole address in the "addr1" field
    street1 = site["addr1"]
    m = re.search(r", Los Angeles, CA\s*(\d{5})?", street1)
    if m:
        street1 = street1[: m.start()]
        city = "Los Angeles"
        zip = m.group(1)

    return schema.Address(
        street1=street1, city=city, state=schema.State.CALIFORNIA, zip=zip
    )


def _get_location(site: dict) -> Optional[schema.LatLng]:
    if site["lat"] and site["lon"]:
        return schema.LatLng(latitude=site["lat"], longitude=site["lon"])

    return None


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    if re.match(r"^https?://", site["link"]):
        contacts.append(schema.Contact(contact_type="booking", website=site["link"]))

    if len(contacts) > 0:
        return contacts

    return None


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    # This is just a guess.
    vaccines = []

    if "j" in site["vaccines"]:
        vaccines.append(
            schema.Vaccine(vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN)
        )

    if "m" in site["vaccines"]:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.MODERNA))

    if "p" in site["vaccines"]:
        vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH))

    if len(vaccines) > 0:
        return vaccines

    return None


def _get_parent_organization(site: dict) -> Optional[schema.Organization]:
    maybe_provider = provider_id_from_name(site["name"])
    if maybe_provider:
        return schema.Organization(id=maybe_provider[0])

    return None


def _get_links(site: dict) -> Optional[List[schema.Link]]:
    maybe_provider = provider_id_from_name(site["name"])
    if maybe_provider:
        return [schema.Link(authority=maybe_provider[0], id=maybe_provider[1])]

    return None


def _get_notes(site: dict) -> Optional[List[str]]:
    notes = []

    for k in ["notes", "alt", "comments", "notesSpn", "altSpn", "commentsSpn"]:
        if site[k]:
            notes.append(site[k])

    if len(notes) > 0:
        return notes

    return None


def _get_active(site: dict) -> Optional[bool]:
    return False if site["inactive"] == "TRUE" else None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    id = location_id_from_name(site["name"])

    return schema.NormalizedLocation(
        id=f"ca_los_angeles_gov:{id}",
        name=site["name"],
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        inventory=_get_inventory(site),
        parent_organization=_get_parent_organization(site),
        links=_get_links(site),
        notes=_get_notes(site),
        active=_get_active(site),
        source=schema.Source(
            source="ca_los_angeles_gov",
            id=id,
            fetched_from_uri="http://publichealth.lacounty.gov/acd/ncorona2019/js/pod-data.js",
            fetched_at=timestamp,
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

                if not parsed_site["name"]:
                    continue

                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
