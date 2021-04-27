#!/usr/bin/env python

import datetime
import json
import pathlib
import re
import sys
from typing import List, Optional

from vaccine_feed_ingest.schema import schema
from vaccine_feed_ingest.utils.normalize import provider_id_from_name


def format_phone(phone: str) -> str:
    ph = re.sub(r"[^0-9]", "", phone)
    if len(ph) == 11:
        ph = ph[1:]
    formatted = "({}) {}-{}".format(ph[0:3], ph[3:6], ph[6:10])
    return formatted


def _get_id(site: dict) -> str:
    return f"immunizenevada_org:{site['id']}"


def _get_title(title: str) -> str:
    """Return normalized title.

    Age restrictions in the form of "(16+)" will be removed. Otherwise,
    the title will be returned unaltered.

    """
    title = re.sub(r"\([0-9]+\+\)", "", title)
    return title


def _get_address(address: str) -> schema.Address:
    parts = address.split(",")
    street2 = None
    if len(parts) == 5:
        street2 = parts[1]

    result = schema.Address(
        street1=parts[0],
        street2=street2,
        city=parts[-3],
        state=parts[-2],
        zip=parts[-1],
    )
    return result


def _get_contacts(site: dict) -> Optional[List[schema.Contact]]:
    contacts = None

    if "contact-phone" in site:
        phone_contact = schema.Contact(
            contact_type="general",
            phone=format_phone(site["contact-phone"]),
        )
        contacts = [phone_contact]

    # Filter out sites with a url of "/"
    if site["url"].startswith("http"):
        web_contact = schema.Contact(
            contact_type="general",
            website=site["url"],
        )
        if contacts is None:
            contacts = []
        contacts.append(web_contact)

    return contacts


def _get_links(site: dict) -> Optional[List[schema.Link]]:
    links = []
    immunize_nv_link = schema.Link(
        authority="immunizenevada_org",
        id=site["title"],
    )
    links.append(immunize_nv_link)

    parsed_provider_link = provider_id_from_name(site["title"])
    if parsed_provider_link is not None:
        provider_link = schema.Link(
            authority=parsed_provider_link[0],
            id=parsed_provider_link[1],
        )
        links.append(provider_link)

    return links


def normalize(site: dict, timestamp: str) -> schema.NormalizedLocation:
    return schema.NormalizedLocation(
        id=_get_id(site),
        name=_get_title(site["title"]),
        address=_get_address(site["address"]),
        location=schema.LatLng(
            latitude=site["lat"],
            longitude=site["lng"],
        ),
        contact=_get_contacts(site),
        languages=None,
        opening_dates=None,
        opening_hours=None,
        availability=None,
        inventory=None,
        access=None,
        parent_organization=None,
        links=_get_links(site),
        notes=None,
        active=None,
        source=schema.Source(
            source="immunizenevada_org",
            id=site["id"],
            fetched_at=timestamp,
            fetched_from_uri="https://www.immunizenevada.org/covid-19-vaccine-locator",
            data=site,
        ),
    )


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    output_dir = pathlib.Path(argv[0])
    input_dir = pathlib.Path(argv[1])
    ndjson_filepaths = input_dir.glob("*.ndjson")
    parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

    for input_file in ndjson_filepaths:
        slug = input_file.name.split(".", maxsplit=1)[0]
        output_file = output_dir / f"{slug}.normalized.ndjson"

        with input_file.open() as in_fh:
            with output_file.open("w") as out_fh:
                for site_json in in_fh:
                    site = json.loads(site_json)
                    normalized_site = normalize(site, parsed_at_timestamp)
                    line = normalized_site.json()
                    out_fh.write(line)
                    out_fh.write("\n")


if __name__ == "__main__":
    sys.exit(main())
