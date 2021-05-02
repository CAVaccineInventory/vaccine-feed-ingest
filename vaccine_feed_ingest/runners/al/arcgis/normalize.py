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
from typing import List, Optional, Tuple

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


def _get_contacts(site: dict) -> Optional[List[location.Contact]]:
    contacts = []
    if site["attributes"]["prereg_phone"]:
        matches = list(
            re.finditer(
                r"(?P<area_code>\d\d\d)\)?-? ?(?P<rest_of_number>\d\d\d-\d\d\d\d)",
                site["attributes"]["prereg_phone"],
            )
        )

        if not matches:
            logger.warning(
                "unparseable phone number: '%s'", site["attributes"]["prereg_phone"]
            )
            return None

        for match in matches:
            phone = f"({match.group('area_code')}) {match.group('rest_of_number')}"
            contacts.append(location.Contact(contact_type="general", phone=phone))

    website = site["attributes"]["prereg_website"]
    if website:
        # this edge case...
        website = website.replace("htttp", "http")
        if "http" not in website:
            website = "https://" + website
        website = website.replace(" ", "")
        contacts.append(location.Contact(contact_type="general", website=website))

    if len(contacts) > 0:
        return contacts

    return None


def _get_languages(site: dict) -> Optional[List[str]]:
    return {None: None, "Yes": ["en", "es"], "No": ["en"]}[
        site["attributes"]["spanish_staff_y_n"]
    ]


def _get_opening_dates(site: dict) -> Optional[List[location.OpenDate]]:
    opens = None
    closes = None
    if site["attributes"]["begindate"] is not None:
        opens = (
            datetime.fromtimestamp(site["attributes"]["begindate"] // 1000)
            .date()
            .isoformat()
        )

    if site["attributes"]["enddate"] is not None:
        closes = (
            datetime.fromtimestamp(site["attributes"]["enddate"] // 1000)
            .date()
            .isoformat()
        )

    if opens is None and closes is None:
        return None

    return [
        location.OpenDate(
            opens=opens,
            closes=closes,
        )
    ]


def _parse_time(human_readable_time: str) -> Tuple[int, int]:
    match = re.match(r"^(?P<hour>\d+):(?P<minute>\d+) ?AM?$", human_readable_time)
    if match:
        return int(match.group("hour")), int(match.group("minute"))

    match = re.match(r"^(?P<hour>\d+):(?P<minute>\d+) ?P[MN]?$", human_readable_time)
    if match:
        return int(match.group("hour")) + 12, int(match.group("minute"))

    match = re.match(r"^(?P<hour>\d+) ?AM$", human_readable_time)
    if match:
        return int(match.group("hour")), 0

    match = re.match(r"^(?P<hour>\d+) ?PM$", human_readable_time)
    if match:
        return int(match.group("hour")) + 12, 0

    match = re.match(r"^(?P<hour>\d+):(?P<minute>\d+)$", human_readable_time)
    if match:
        return int(match.group("hour")), int(match.group("minute"))

    raise ValueError(human_readable_time)


def _normalize_time(human_readable_time: str) -> str:
    hour, minute = _parse_time(human_readable_time)
    return str(hour % 24).rjust(2, "0") + ":" + str(minute).rjust(2, "0")


def _normalize_hours(
    human_readable_hours: Optional[str], day: str
) -> List[location.OpenHour]:
    processed_hours = human_readable_hours
    if processed_hours is None:
        return []

    if processed_hours == "8-4":
        return [location.OpenHour(day=day, opens="08:00", closes="16:00")]
    if processed_hours == "8:00AM7:00PM":
        return [location.OpenHour(day=day, opens="08:00", closes="16:00")]

    processed_hours = processed_hours.upper().lstrip("BY APPOINTMENT ").strip()

    if " AND " in processed_hours:
        ranges = processed_hours.split(" AND ")
        return sum((_normalize_hours(hours_range, day) for hours_range in ranges), [])

    if ";" in processed_hours:
        ranges = processed_hours.split(";")
        return sum((_normalize_hours(hours_range, day) for hours_range in ranges), [])

    if " TO " in processed_hours:
        processed_hours = processed_hours.replace(" TO ", "-")

    if processed_hours.count("-") != 1:
        logger.warning("unparseable hours: '%s'", human_readable_hours)
        return []

    open_time, close_time = processed_hours.split("-")
    try:
        return [
            location.OpenHour(
                day=day,
                opens=_normalize_time(open_time.strip().upper()),
                closes=_normalize_time(close_time.strip().upper()),
            )
        ]
    except ValueError:
        logger.warning("unparseable hours: '%s'", human_readable_hours)
        return []


def _get_opening_hours(site: dict) -> Optional[List[location.OpenHour]]:
    hours = []

    if site["attributes"]["mon_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["mon_hrs"], "monday")

    if site["attributes"]["tues_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["tues_hrs"], "tuesday")

    if site["attributes"]["wed_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["wed_hrs"], "wednesday")

    if site["attributes"]["thurs_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["thur_hrs"], "thursday")

    if site["attributes"]["fri_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["fri_hrs"], "friday")

    if site["attributes"]["sat_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["sat_hrs"], "saturday")

    if site["attributes"]["sun_open"] == "Yes":
        hours += _normalize_hours(site["attributes"]["sun_hrs"], "sunday")

    return hours if hours else None


def _get_inventory(site: dict) -> Optional[List[location.Vaccine]]:
    inventory_str = site["attributes"]["vaccine_manufacturer"]
    inventory = (
        inventory_str.split(";") if ";" in inventory_str else inventory_str.split(",")
    )

    return [
        {
            "Pfizer_BioNTech": location.Vaccine(vaccine="pfizer_biontech"),
            "Pfizer-BioNTech": location.Vaccine(vaccine="pfizer_biontech"),
            "Pfizer": location.Vaccine(vaccine="pfizer_biontech"),
            "Moderna": location.Vaccine(vaccine="moderna"),
            "J_J": location.Vaccine(vaccine="johnson_johnson_janssen"),
        }[vaccine.lstrip("\u200b").strip()]
        for vaccine in inventory
    ]


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
                street2=site["attributes"]["GEO_ADDRESS"].title(),
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


"""
def normalize_federal_partners_sites(
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
                fetched_from_uri=FETCHED_FROM_URI,  # noqa: E501
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
            print("check the file above\n\n\n")
            normalize_providers_sites(in_filepath, out_filepath, timestamp)
        """
        elif layer_id == "8f23e1c3b5c54198ab60d2f729cb787d":
            normalize_federal_partners_sites(in_filepath, out_filepath, timestamp)
        elif layer_id == "d1a799c7f98e41fb8c6b4386ca6fe014":
            normalize_appt_only_2_sites(in_filepath, out_filepath, timestamp)
        elif layer_id == "8537322b652841b4a36b7ddb7bc3b204":
            normalize_drive_thru_walk_in_sites(in_filepath, out_filepath, timestamp)
        else:
            logger.warning("Unable to process layer with id: %s", layer_id)
        """


if __name__ == "__main__":
    main()
