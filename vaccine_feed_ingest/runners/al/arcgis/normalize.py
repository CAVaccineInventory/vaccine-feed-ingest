#!/usr/bin/env python3
# isort: skip_file
"""
Notes to reviewers:
    - There a four different input file structures. site_attribute_lookup is
      is a dictionary to map the attributes within these different file stuctures
      to something more unified.
{
  "id": "45748",
  "name": "Rite Aid #05952",
  "address": {
    "street1": "1991 Mountain Boulevard",
    "city": "Oakland",
    "state": "CA",
    "zip": "94611",
  },
  "location": {
    "latitude": 37.8273167,
    "longitude": -122.2105179,
  },
  "contact": [
    {
      "contact_type": "booking",
      "website": "https://www.riteaid.com/pharmacy/covid-qualifier",
    },
    {
      "contact_type": "general",
      "phone": "(510) 339-2215",
      "website": "https://www.riteaid.com/locations/ca/oakland/1991-mountain-boulevard.html",
    },
  ],
  "availability": {
    "appointments": true,
    "drop_in": false,
  },
}

    - Source layer with corresponding IDs
        layer: Providers, id: 51d4c310f1fe4d83a63e2b47acb77898
        layer: FederalPartners, id: 8f23e1c3b5c54198ab60d2f729cb787d
        layer: ApptOnly2, id: d1a799c7f98e41fb8c6b4386ca6fe014
        layer: DriveThruWalkIn, id: 8537322b652841b4a36b7ddb7bc3b204
"""


import json
import logging
import os
import pathlib
import re
import sys
from datetime import datetime
from typing import Optional, List

from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils.validation import BOUNDING_BOX

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)
logger = logging.getLogger("al/arcgis/normalize.py")

STATE = "AL"
SOURCE_NAME = "al_arcgis"
FETCHED_FROM_URI = "https://alpublichealth.maps.arcgis.com/apps/opsdashboard/index.html#/2b4627aa70c5450791a7cf439ed047ec"


def _get_id(data_id: str, layer_id: str,) -> str:
    return f"al_arcgis:{layer_id}_0_{data_id}"


def _get_lat_lng(site: dict) -> Optional[location.LatLng]:
    lat_lng = location.LatLng(
        latitude=site["geometry"]["y"], longitude=site["geometry"]["x"]
    )

    if not BOUNDING_BOX.latitude.contains(
        lat_lng.latitude
    ) or not BOUNDING_BOX.longitude.contains(lat_lng.longitude):
        return None

    return lat_lng


def normalize_providers_sites(
    in_filepath: pathlib.Path,
    out_filepath: pathlib.Path,
    timestamp: str
) -> None:

    def _get_normalized_site(site: dict, timestamp: str) -> location.NormalizedLocation:
        return location.NormalizedLocation(
            id=_get_id(site["attributes"]["OBJECTID"], "51d4c310f1fe4d83a63e2b47acb77898"),
            name=site["attributes"]["SITE_NAME"].title(),
            address=location.Address(
                street1=site["attributes"]["Match_addr"],
                street2=None,
                city=site["attributes"]["CITY"].title(),
                state=STATE,
                zip=str(site["attributes"]["ID_ZIPCODE"]),
            ),
            location=_get_lat_lng(site),
            source=location.Source(
                source=SOURCE_NAME,
                id=site["attributes"]["OBJECTID"],
                fetched_from_uri=FETCHED_FROM_URI,
                fetched_at=timestamp,
                data=site,
            ),
        )

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for entry in fin:
                site = json.loads(entry)

                normalized_site = _get_normalized_site(
                    site, timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")


def normalize_federal_partners_sites(
    in_filepath: pathlib.Path,
    out_filepath: pathlib.Path,
    timestamp: str
) -> None:

    def _get_normalized_site(site: dict, timestamp: str) -> location.NormalizedLocation:
        return location.NormalizedLocation(
            id=_get_id(site["attributes"]["objectId"], "8f23e1c3b5c54198ab60d2f729cb787d"),
            name=site["attributes"]["f2"],
            address=location.Address(
                street1=site["attributes"]["f3"],
                street2=None,
                city=site["attributes"]["f4"].title(),
                state=STATE,
            ),
            location=_get_lat_lng(site),
            source=location.Source(
                source=SOURCE_NAME,
                id=site["attributes"]["objectId"],
                fetched_from_uri=FETCHED_FROM_URI,
                fetched_at=timestamp,
                data=site,
            ),
        )

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for entry in fin:
                site = json.loads(entry)

                normalized_site = _get_normalized_site(
                    site, timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")


def normalize_appt_only_2_sites(
    in_filepath: pathlib.Path,
    out_filepath: pathlib.Path,
    timestamp: str
) -> None:
    def _get_contact(site: dict) -> Optional[List[location.Contact]]:
        click_here_field = site["attributes"]["f6"]
        regex = re.search("(?P<url>https?://[^\s'\"]+)", click_here_field)
        if regex:
            url = regex.group("url")
            return [
                location.Contact(
                    contact_type="booking",
                    website=url
                )
            ]
        else:
            return None

    def _get_normalized_site(site: dict, timestamp: str) -> location.NormalizedLocation:
        return location.NormalizedLocation(
            id=_get_id(site["attributes"]["objectId"], "d1a799c7f98e41fb8c6b4386ca6fe014"),
            name=site["attributes"]["f3"],
            address=None,
            location=_get_lat_lng(site),
            contact=_get_contact(site),
            availability=location.Availability(
                drop_in=False,
                appointments=True,
            ),
            source=location.Source(
                source=SOURCE_NAME,
                id=site["attributes"]["objectId"],
                fetched_from_uri=FETCHED_FROM_URI,
                fetched_at=timestamp,
                data=site,
            ),
        )

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for entry in fin:
                site = json.loads(entry)

                normalized_site = _get_normalized_site(
                    site, timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")


"""
def normalize_drive_thru_walk_in_sites(
    in_filepath: pathlib.Path,
    out_filepath: pathlib.Path,
    timestamp: str
) -> None:
    def _get_normalized_site(site: dict, timestamp: str) -> location.NormalizedLocation:
        return location.NormalizedLocation(
            id=_get_id(site),
            name=site["attributes"]["loc_name"],
            address=location.Address(
                street1=site["attributes"]["addr1"],
                street2=site["attributes"]["addr2"],
                city=site["attributes"]["city"],
                state="AZ",
                zip=site["attributes"]["zip"],
            ),
            location=_get_lat_lng(site),
            contact=_get_contacts(site),
            languages=_get_languages(site),
            opening_dates=_get_opening_dates(site),
            opening_hours=_get_opening_hours(site),
            availability=None,
            inventory=_get_inventory(site),
            access=None,
            parent_organization=None,
            links=None,
            notes=[site["attributes"]["prereg_comments"]]
            if site["attributes"]["prereg_comments"]
            else None,
            active=None,
            source=location.Source(
                source="az_arcgis",
                id=site["attributes"]["globalid"],
                fetched_from_uri="https://adhsgis.maps.arcgis.com/apps/opsdashboard/index.html#/5d636af4d5134a819833b1a3b906e1b6",  # noqa: E501
                fetched_at=timestamp,
                data=site,
            ),
        )

    with in_filepath.open() as fin:
        with out_filepath.open("w") as fout:
            for entry in fin:
                site = json.loads(entry)

                normalized_site = _get_normalized_site(
                    site, timestamp
                )

                json.dump(normalized_site.dict(), fout)
                fout.write("\n")
"""


def main():
    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])

    json_filepaths = input_dir.glob("*.ndjson")
    timestamp = datetime.utcnow().isoformat()

    for in_filepath in json_filepaths:
        filename, _ = os.path.splitext(in_filepath.name)
        out_filepath = output_dir / f"{filename}.normalized.ndjson"
        layer_id = filename.split("_")[0]

        logger.info(
            "normalizing %s => %s",
            in_filepath,
            out_filepath,
        )

        if layer_id == "51d4c310f1fe4d83a63e2b47acb77898":
            normalize_providers_sites(in_filepath, out_filepath, timestamp)
        elif layer_id == "8f23e1c3b5c54198ab60d2f729cb787d":
            normalize_federal_partners_sites(in_filepath, out_filepath, timestamp)

        elif layer_id == "d1a799c7f98e41fb8c6b4386ca6fe014":
            normalize_appt_only_2_sites(in_filepath, out_filepath, timestamp)
        """
        elif layer_id == "8537322b652841b4a36b7ddb7bc3b204":
            normalize_drive_thru_walk_in_sites(in_filepath, out_filepath, timestamp)
        else:
            logger.warning("Unable to process layer with id: %s", layer_id)
        """


if __name__ == "__main__":
    main()
