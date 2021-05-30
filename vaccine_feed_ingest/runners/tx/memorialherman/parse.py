#!/usr/bin/env python3

import json
import os
import pathlib
import re
import sys
from typing import List, Tuple

from bs4 import BeautifulSoup, element

DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


def parse_address(p: element.Tag) -> str:
    address_string = p.get_text()
    address = address_string.replace('Address', '')
    address = address.strip()

    return address


def parse_vaccine(p: element.Tag) -> str:
    vaccine_string = p.get_text()

    vaccine_string = vaccine_string.replace('Vaccine', '')
    vaccine_string = vaccine_string.strip()

    return vaccine_string


def parse_hours(p: element.Tag) -> List[Tuple[str, List[str]]]:
    hour_string = p.get_text()
    hour_string = hour_string.replace('Walk-in Hours', '')
    
    lines = hour_string.split('\n')

    days_to_times = []

    current_days = None
    current_times = []

    for line in lines:
        if len(line.strip()) == 0:
            continue

        is_date_line = any(day in line for day in DAYS)
       
        if is_date_line:
            if current_days is not None:
                days_to_times.append((current_days, current_times))
            current_days = line.strip()
            current_times = []
        else:
            current_times.append(line.strip())

    if current_days is not None:
        days_to_times.append((current_days, current_times))

    return days_to_times


def parse_details(p: element.Tag) -> str:
    detail_string = p.get_text()
    detail_string = detail_string.replace('Location Details', '')
    detail_string = detail_string.replace('View Location Page »', '')
    detail_string = detail_string.strip()

    return detail_string


input_dir = pathlib.Path(sys.argv[2])
in_file_path = input_dir / "memorialherman.html"
output_dir = pathlib.Path(sys.argv[1])
out_file_path = output_dir / "data.parsed.ndjson"

site_data = {'sites': []}
with in_file_path.open() as in_file:
    content = in_file.read()

soup = BeautifulSoup(content, 'html.parser')

main_section = soup.find(attrs={'class': 'generic rte'})


current_site = None

for item in main_section.children:
    # Toss empty lines
    if item.name == 'h3':
        if current_site is not None:
            site_data['sites'].append(current_site)

        current_site = {'name': item.contents}
    # Skip lines before sites are listed
    elif current_site is None:
        continue
    elif item.name == 'p':
        header = item.find('strong')

        # Not all address sections have headers
        if header is None:
            header_text = 'Address'
        else:
            header_text = header.get_text()

        if 'Address' in header_text:
            address = parse_address(item)
            current_site['address'] = address

        elif 'Walk-in Hours' in header_text:
            hours = parse_hours(item)
            current_site['hours'] = hours

        elif 'Vaccine' in header_text:
            vaccine = parse_vaccine(item)
            current_site['vaccine'] = vaccine

        elif 'Location Details' in header_text:
            location_details = parse_details(item)
            current_site['location_details'] = location_details

with open(out_file_path, 'w') as out_file:
    json.dump(site_data, out_file)
    out_file.write("\n")
