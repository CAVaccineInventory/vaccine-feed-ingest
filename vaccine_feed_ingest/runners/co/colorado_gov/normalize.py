#!/usr/bin/env python

import datetime
import json
import os
import pathlib
import re
import sys
import urllib
from typing import List, Optional

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import normalize_url, provider_id_from_name
from vaccine_feed_ingest.utils.parse import location_id_from_name

logger = getLogger(__file__)


def _get_id(site: dict) -> str:
    if "ExtendedData" in site and "PIN" in site["ExtendedData"]:
        return location_id_from_name(site["ExtendedData"]["PIN"])

    return location_id_from_name(site["name"])


def _get_address(site: dict) -> schema.Address:
    # Example addresses:
    #
    # "No 6 Town Plaza Shopping Ctr, Durango, CO"
    # "1900 18th Street, 1st Floor, Denver, CO"
    # "2121 S Oneida St Suite 200, Denver, CO, 80224"
    #
    # Assuming `street1 "," city "," state ["," zip]`,
    # where street1 may contain commas

    street1 = city = zip = None

    if "address" in site:
        parts = list(map(lambda part: part.strip(), site["address"].split(",")))
        zip = parts.pop() if len(parts) == 4 and re.match(r"\d{5}$", parts[3]) else None
        parts.pop()  # "CO"
        city = parts.pop()
        street1 = ", ".join(parts)

    return schema.Address(
        street1=street1, city=city, state=schema.State.COLORADO, zip=zip
    )


def _get_location(site: dict) -> Optional[schema.LatLng]:
    if "Location" in site:
        return schema.Location(
            latitude=site["Location"]["lat"], longitude=site["Location"]["long"]
        )

    return None


def _normalize_websites(maybe_websites: str) -> List[str]:
    websites = []

    for website in maybe_websites.lower().split(" "):
        if not re.match(r"https?://", website):
            if website[0:4] == "www." or re.match(r"\.(com|net|org|gov)", website[:-4]):
                website = "http://" + website
        if re.match(r"https?://", website):
            websites.append(normalize_url(website))

    return websites


def _get_provider_store_page(site: dict) -> Optional[str]:
    """
    Get the webpage for a specific store if possible,
    or a store listing for all stores in the state.

    """

    provider = provider_id_from_name(site["name"])

    if provider and provider[0] == schema.VaccineProvider.COSTCO:
        return "https://www.costco.com/warehouse-locations"

    if provider and provider[0] == schema.VaccineProvider.CVS:
        return "https://www.cvs.com/store-locator/cvs-pharmacy-locations/Colorado"

    if provider and provider[0] == schema.VaccineProvider.KING_SOOPERS:
        return f"https://www.kingsoopers.com/stores/details/620/{provider[1]}"

    if provider and provider[0] == schema.VaccineProvider.SAMS:
        return "https://www.samsclub.com/locator"

    if provider and provider[0] == schema.VaccineProvider.WALMART:
        return f"https://www.walmart.com/store/{provider[1]}"

    m = re.match(r"City Market Pharmacy 625(\d{5})", site["name"])
    if m:
        return f"https://www.citymarket.com/stores/details/620/{m.group(1)}"

    if site["name"] == "Walgreen Drug Store":
        return "https://www.walgreens.com/storelistings/storesbycity.jsp?requestType=locator&state=CO"

    if site["name"] == "Pharmaca Integrative Pharmacy":
        return "https://www.pharmacarx.com/pharmacy-locator/"

    if "address" in site and site["address"]:
        if site["name"] == "Safeway Pharmacy":
            return f"https://local.safeway.com/search.html?q={urllib.parse.quote(site['address'])}&storetype=5657&l=en"

        if site["name"][0:17] == "The Little Clinic":
            return f"https://www.thelittleclinic.com/clinic-locator?searchText={urllib.parse.quote(site['address'])}"

    return None


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = []

    if "ExtendedData" in site:
        if "phone" in site["ExtendedData"] and site["ExtendedData"]["phone"]:
            phone = re.sub(r"\D", "", site["ExtendedData"]["phone"])
            if re.match(r"1?\d{10}$", phone):
                if len(phone) == 11:
                    phone = phone[1:]
                contacts.append(
                    schema.Contact(
                        contact_type=schema.ContactType.GENERAL,
                        phone=f"({phone[0:3]}) {phone[3:6]}-{phone[6:10]}",
                    )
                )

        if "website" in site["ExtendedData"] and site["ExtendedData"]["website"]:
            for website in _normalize_websites(site["ExtendedData"]["website"]):
                contacts.append(
                    schema.Contact(
                        contact_type=schema.ContactType.GENERAL, website=website
                    )
                )

        if (
            "vaccine sign up" in site["ExtendedData"]
            and site["ExtendedData"]["vaccine sign up"]
        ):
            for website in _normalize_websites(site["ExtendedData"]["vaccine sign up"]):
                contacts.append(
                    schema.Contact(
                        contact_type=schema.ContactType.BOOKING, website=website
                    )
                )
    else:
        # Community vaccination sites have no "ExtendedData"; the contact info is in free-form notes
        contacts.append(
            schema.Contact(
                contact_type=schema.ContactType.GENERAL,
                other="\n".join(site["description"]),
            )
        )

    if len(contacts) == 0:
        # Try to guess the website for major brands
        website = _get_provider_store_page(site)
        if website:
            contacts.append(
                schema.Contact(contact_type=schema.ContactType.GENERAL, website=website)
            )

    if len(contacts) > 0:
        return contacts

    return None


def _get_inventory(site: dict) -> Optional[List[schema.Vaccine]]:
    vaccines = []

    if "ExtendedData" in site:
        have_moderna = site["_folder_name"] == "Moderna"
        have_pfizer = site["_folder_name"] == "Pfizer"
        for field in ["Description", "Mapped Description", "unnamed (1)"]:
            if field in site["ExtendedData"]:
                if "moderna" in site["ExtendedData"][field].lower():
                    have_moderna = True
                if "pfizer" in site["ExtendedData"][field].lower():
                    have_pfizer = True
        if have_moderna:
            vaccines.append(schema.Vaccine(vaccine=schema.VaccineType.MODERNA))
        if have_pfizer:
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

    if "ExtendedData" in site:
        if (
            "registrtiondescription" in site["ExtendedData"]
            and site["ExtendedData"]["registrtiondescription"]
            and site["ExtendedData"]["registrtiondescription"] != "#N/A"
        ):
            notes.append(site["ExtendedData"]["registrtiondescription"])
        if (
            "spanishresources" in site["ExtendedData"]
            and site["ExtendedData"]["spanishresources"]
            and site["ExtendedData"]["spanishresources"] != "#N/A"
        ):
            notes.append("Spanish: " + site["ExtendedData"]["spanishresources"])

    if len(notes) > 0:
        return notes

    return None


def _get_normalized_location(site: dict, timestamp: str) -> schema.NormalizedLocation:
    source = "co_colorado_gov"
    id = _get_id(site)

    return schema.NormalizedLocation(
        id=f"{source}:{id}",
        name=site["name"],
        address=_get_address(site),
        location=_get_location(site),
        contact=_get_contacts(site),
        inventory=_get_inventory(site),
        parent_organization=_get_parent_organization(site),
        links=_get_links(site),
        notes=_get_notes(site),
        source=schema.Source(
            source=source,
            id=id,
            fetched_from_uri="https://www.google.com/maps/d/viewer?mid=1x9KT3SJub0igOTnhFtdRYmceZuBXMWvK&ll=38.98747216882165,-105.9556642978642&z=6",
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
