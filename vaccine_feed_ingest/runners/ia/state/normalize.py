#!/usr/bin/env python3
import datetime
import json
import pathlib
import sys

from pydantic import ValidationError
from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils.log import getLogger

logger = getLogger(__file__)

SOURCE_ID = "ia_state"


def zip_infile_outfile_pairs(input_dir, output_dir):
    infile_paths = input_dir.glob("*.ndjson")
    for infile_path in infile_paths:
        filename = infile_path.stem.partition(".")[0]
        outfile_path = output_dir.joinpath(f"{filename}.normalized.ndjson")
        yield infile_path, outfile_path


def _get_contacts(record):
    contacts = []
    try:
        if record.get("websiteurl"):
            contacts.append(location.Contact(website=record["websiteurl"]))
    except ValidationError as e:
        logger.error(f"Ignoring invalid website '{record['websiteurl']}':\n{str(e)}")

    try:
        if record.get("Phone"):
            contacts.append(location.Contact(phone=record["Phone"]))
    except ValidationError as e:
        logger.error(f"Ignoring invalid phone number '{record['Phone']}':\n{str(e)}")
    return contacts


def convert_to_schema(record, timestamp):
    return location.NormalizedLocation(
        id=f"{SOURCE_ID}:{record['ProviderID']}",
        name=record["ProviderName"],
        address=location.Address(
            street1=record["Address1"],
            city=record["City"],
            state=record["State"],
            zip=record["Zipcode"],
        ),
        location=location.LatLng(
            latitude=record["LatCoord"], longitude=record["LngCoord"]
        ),
        active=record["ActiveFlag"],
        contact=_get_contacts(record) or None,
        source=location.Source(
            id=record["ProviderID"],
            source=SOURCE_ID,
            fetched_from_uri="https://vaccinate.iowa.gov/providers/SearchProviders",
            fetched_at=timestamp,
            data=record,
        ),
    )


def main(argv):
    output_dir = pathlib.Path(argv[1])
    input_dir = pathlib.Path(argv[2])

    parsed_at_timestamp = datetime.datetime.utcnow().isoformat()

    for infile_path, outfile_path in zip_infile_outfile_pairs(input_dir, output_dir):
        logger.info("normalizing %s => %s", infile_path, outfile_path)
        with open(infile_path, "r") as fin:
            with open(outfile_path, "w") as fout:
                for site_json in fin:
                    parsed_site = json.loads(site_json)
                    normalized_size = convert_to_schema(
                        parsed_site, parsed_at_timestamp
                    )
                    json.dump(normalized_size.dict(), fout)
                    fout.write("\n")


if __name__ == "__main__":
    main(sys.argv)
