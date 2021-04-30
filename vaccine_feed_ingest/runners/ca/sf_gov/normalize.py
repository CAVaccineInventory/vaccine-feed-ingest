#!/usr/bin/env python

import datetime
import json
import pathlib
import sys

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.normalize import provider_id_from_name


def normalize(site: dict, timestamp: str) -> dict:
    address = site["location"]["address"]
    address_parts = [p.strip() for p in address.split(",")]

    # Remove city from end of address
    address_parts.pop()
    street1 = address_parts[0]
    street2 = None
    if len(address_parts) > 1:
        street2 = ", ".join(address_parts[1:])

    links = [schema.Link(authority="sf_gov", id=site["id"])]

    parsed_provider_link = provider_id_from_name(site["name"])
    if parsed_provider_link is not None:
        links.append(
            schema.Link(authority=parsed_provider_link[0], id=parsed_provider_link[1])
        )

    contacts = []

    if site["booking"]["phone"] and site["booking"]["phone"].lower() != "none":
        contacts.append(
            schema.Contact(contact_type="booking", phone=site["booking"]["phone"])
        )

    if site["booking"]["url"] and site["booking"]["url"].lower() != "none":
        contacts.append(
            schema.Contact(contact_type="booking", website=site["booking"]["url"])
        )

    if site["booking"]["info"] and site["booking"]["info"].lower() != "none":
        contacts.append(
            schema.Contact(contact_type="booking", other=site["booking"]["info"])
        )

    return schema.NormalizedLocation(
        id=f"sf_gov:{site['id']}",
        name=site["name"],
        address=schema.Address(
            street1=street1,
            street2=street2,
            city=site["location"]["city"],
            state="CA",
            zip=(
                site["location"]["zip"]
                if site["location"]["zip"] and site["location"]["zip"].lower() != "none"
                else None
            ),
        ),
        location=schema.LatLng(
            latitude=site["location"]["lat"],
            longitude=site["location"]["lng"],
        ),
        contact=contacts,
        languages=[k for k, v in site["access"]["languages"].items() if v],
        opening_dates=None,
        opening_hours=None,
        availability=schema.Availability(
            appointments=site["appointments"]["available"],
            drop_in=site["booking"]["dropins"],
        ),
        inventory=None,
        access=schema.Access(
            walk=site["access_mode"]["walk"],
            drive=site["access_mode"]["drive"],
            wheelchair="yes" if site["access"]["wheelchair"] else "no",
        ),
        parent_organization=None,
        links=links,
        notes=None,
        active=site["active"],
        source=schema.Source(
            source="sf_gov",
            id=site["id"],
            fetched_from_uri="https://vaccination-site-microservice.vercel.app/api/v1/appointments",  # noqa: E501
            fetched_at=timestamp,
            published_at=site["appointments"]["last_updated"],
            data=site,
        ),
    ).dict()


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

json_filepaths = input_dir.glob("*.ndjson")

parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

for in_filepath in json_filepaths:
    filename = in_filepath.name.split(".", maxsplit=1)[0]
    out_filepath = output_dir / f"{filename}.normalized.ndjson"

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for site_json in fin:
                parsed_site = json.loads(site_json)

                normalized_site = normalize(parsed_site, parsed_at_timestamp)

                json.dump(normalized_site, fout)
                fout.write("\n")
