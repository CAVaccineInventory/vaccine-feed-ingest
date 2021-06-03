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
from vaccine_feed_ingest.utils.normalize import normalize_url, provider_id_from_name

logger = getLogger(__file__)


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    if "phone" in site["contact"] and re.match(
        r"^\(\d{3}\) \d{3}-\d{4}$", site["contact"]["phone"]
    ):
        contacts.append(
            schema.Contact(contact_type="general", phone=site["contact"]["phone"])
        )

    if "website" in site["contact"]:
        uri = site["contact"]["website"]
        if uri[0:7] == "mailto:":
            contacts.append(schema.Contact(contact_type="general", email=uri[7:]))
        else:
            contacts.append(
                schema.Contact(contact_type="general", website=normalize_url(uri))
            )

    if len(contacts) > 0:
        return contacts

    return None


def _get_address(site: dict) -> schema.Address:
    return schema.Address(
        street1=site["address"]["street1"],
        city=site["address"]["city"],
        state=site["address"]["state"],
    )


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


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=f"la_tableau:{site['id']}",
        name=site["name"],
        address=_get_address(site),
        contact=_get_contacts(site),
        parent_organization=_get_parent_organization(site),
        links=_get_links(site),
        source=schema.Source(
            source="la_tableau",
            id=site["id"],
            fetched_from_uri="https://public.tableau.com/profile/lee.mendoza#!/vizhome/pharmacies_desktop/Pharmacies_desktop",
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

                normalized_site = _get_normalized_location(
                    parsed_site, parsed_at_timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
