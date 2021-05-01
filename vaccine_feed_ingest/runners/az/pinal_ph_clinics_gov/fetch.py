#!/usr/bin/env python

import asyncio
import pathlib
import sys

from aiohttp import ClientSession

START_URL = "https://www.pinalcountyaz.gov/publichealth/Pages/OfficeLocations.aspx"


async def main(argv):
    output_dir = pathlib.Path(argv[0])
    output_file = output_dir / "office-locations.html"

    async with ClientSession() as session:
        async with session.get(START_URL) as response:
            contents = await response.text()
            with output_file.open("w") as f:
                f.write(contents)
                f.write("\n")


# If this file is being run from the CLI instead of imported as a module
if __name__ == "__main__":
    # discard the first item in sys.argv as it's the script name.
    # Example: '.../fetch.py'
    argv = sys.argv[1:]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(argv))
