#!/usr/bin/env python

import datetime
import json
import logging
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

logger = logging.getLogger(__name__)

RUNNER_ID = "az_pinal_ph_vaccinelocations_gov"


def _get_id(site: dict) -> str:
    id = f"{_get_name(site)}_{_get_city(site)}".lower()
    id = id.replace(" ", "_").replace(".", "_").replace("\u2019", "_")
    id = id.replace("(", "_").replace(")", "_").replace("/", "_")
    return id


def _get_name(site: dict) -> str:
    return site["providerName"]


def _get_city(site: dict) -> str:
    return site["city"].lstrip().rstrip()


# address is loosely structured and inconsistent, so we're going to bash our
# way through it, mostly parsing from the end of the string
def _get_address(site: dict) -> Optional[schema.Address]:
    if "address" not in site or not site["address"]:
        return None

    address = site["address"]
    address = re.sub("\s+", " ", address)
    address = re.sub("\s*,+", ",", address)
    address = address.strip()

    # pull a zip code off the end
    zip = None
    if match := re.search(" (\\d\\d\\d\\d\\d-\\d\\d\\d\\d)$", address):
        zip = match.group(1)
        address = address.rstrip(f" {zip}")
    if match := re.search(" (\\d\\d\\d\\d\\d)$", address):
        zip = match.group(1)
        address = address.rstrip(f" {zip}")

    state = "AZ"
    address = address.rstrip()
    address = address.rstrip(",")
    address = address.rstrip(".")
    address = address.rstrip(f" {state}")
    address = address.rstrip()
    address = address.rstrip(",")
    address = address.rstrip(f" {_get_city(site)}")
    address = address.rstrip()
    address = address.rstrip(",")

    address_split = address.split(",")
    street1 = address_split[0]
    street2 = ", ".join(address_split[1:]) if len(address_split) > 1 else None

    return schema.Address(
        street1=street1,
        street2=street2,
        city=_get_city(site),
        state=state,
        zip=zip,
    )


def _get_contacts(site: dict) -> schema.Contact:
    ret = []

    if "phoneNumber" in site and site["phoneNumber"]:
        raw_phone = str(site["phoneNumber"]).lstrip("1").lstrip("-")
        if raw_phone[3] == "-" or raw_phone[7] == "-":
            phone = "(" + raw_phone[0:3] + ") " + raw_phone[4:7] + "-" + raw_phone[8:12]
        elif len(raw_phone) == 10:
            phone = "(" + raw_phone[0:3] + ") " + raw_phone[3:6] + "-" + raw_phone[6:10]
        else:
            phone = raw_phone[0:14]

        ret.append(schema.Contact(phone=phone))

    if "website" in site and site["website"]:
        ret.append(schema.Contact(website=site["website"]))

    return ret


def _get_inventories(site: dict) -> List[schema.Vaccine]:
    ret = []
    if "vaccineType" in site and site["vaccineType"]:
        if "Moderna" in site["vaccineType"]:
            ret.append(schema.Vaccine(vaccine=schema.VaccineType.MODERNA))
        if "Pfizer" in site["vaccineType"]:
            ret.append(schema.Vaccine(vaccine=schema.VaccineType.PFIZER_BIONTECH))
        if "Janssen" in site["vaccineType"]:
            ret.append(
                schema.Vaccine(vaccine=schema.VaccineType.JOHNSON_JOHNSON_JANSSEN)
            )
    return ret


def _get_organization(site: dict) -> Optional[schema.Organization]:
    if "CVS" in _get_name(site):
        return schema.Organization(id=schema.VaccineProvider.CVS)
    if "Safeway" in _get_name(site):
        return schema.Organization(id=schema.VaccineProvider.SAFEWAY)
    if "Walgreens" in _get_name(site):
        return schema.Organization(id=schema.VaccineProvider.WALGREENS)
    if "Walmart" in _get_name(site):
        return schema.Organization(id=schema.VaccineProvider.WALMART)
    return None


def _get_source(site: dict, timestamp: str) -> schema.Source:
    return schema.Source(
        data=site,
        fetched_at=timestamp,
        fetched_from_uri="https://www.pinalcountyaz.gov/publichealth/CoronaVirus/Pages/vaccinelocations.aspx",
        id=_get_id(site),
        source=RUNNER_ID,
    )


def normalize(site: dict, timestamp: str) -> str:
    normalized = schema.NormalizedLocation(
        id=f"{RUNNER_ID}:{_get_id(site)}",
        name=_get_name(site),
        address=_get_address(site),
        contact=_get_contacts(site),
        inventory=_get_inventories(site),
        parent_organization=_get_organization(site),
        source=_get_source(site, timestamp),
    ).dict()
    return normalized


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

            json.dump(normalized_site, fout)
            fout.write("\n")
