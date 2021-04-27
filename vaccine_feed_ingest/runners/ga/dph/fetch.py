#!/usr/bin/env python

import pathlib
import sys
import requests


def main():
    output_dir = pathlib.Path(sys.argv[1])
    output_file = output_dir / "locations.html"

    response = requests.get("https://dph.georgia.gov/locations/covid-vaccination-site")
    with output_file.open("w") as f:
        f.write(response.text)
        f.write("\n")


if __name__ == "__main__":
    sys.exit(main())
