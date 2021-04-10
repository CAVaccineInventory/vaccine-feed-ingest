#!/usr/bin/env python

import json
import pathlib
import sys
from datetime import datetime, timezone

site_dir = pathlib.Path(__file__).parent
state_dir = site_dir.parent
runner_dir = state_dir.parent
root_dir = runner_dir.parent

sys.path.append(str(root_dir))
from normalizers import jsonschema  # noqa: E402

ndjson_file = sys.argv[1]
normalized_file = sys.argv[2]

with open(normalized_file, "w") as fout:
    now = datetime.now(timezone.utc).isoformat()

    with open(ndjson_file) as fin:
        for line in fin:
            obj = json.loads(line)

            address = obj["location"]["address"]
            address_parts = address.split(", ")

            city = address_parts.pop()
            street1 = address_parts[0]
            street2 = None
            if len(address_parts) > 1:
                street2 = ", ".join(address_parts[1:])

            zip = obj["location"]["zip"]
            lat = obj["location"]["lat"]
            long = obj["location"]["lng"]
            uri = "https://vaccination-site-microservice.vercel.app/api/v1/appointments"

            location = jsonschema.Location(
                id=f"sf_gov:{obj['id']}",
                name=obj["name"],
                street1=street1,
                street2=street2,
                city=city,
                zip=zip,
                latitude=lat,
                longitude=long,
                booking_website=obj["booking"]["url"],
                booking_phone=obj["booking"]["phone"],
                appointments_available=obj["appointments"]["available"],
                fetched_at=now,  # this is actually time parsed, not fetched
                fetched_from_uri=uri,
                published_at=obj["appointments"]["last_updated"],
                source="sf_gov",
                data=obj,
            )

            d = jsonschema.to_dict(location)
            json.dump(d, fout)
            fout.write("\n")
