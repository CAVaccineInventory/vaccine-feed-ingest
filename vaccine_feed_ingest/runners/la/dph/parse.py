#!/usr/bin/env python

import json
import pathlib
import sys

from bs4 import BeautifulSoup

from vaccine_feed_ingest.utils.parse import location_id_from_name


def detect_vaccine_types(string):
    vaccines = {"pfizer": None, "moderna": None, "janssen": None}
    if "moderna" in string:
        vaccines["moderna"] = True
    elif "pfizer" in string:
        vaccines["pfizer"] = True
    elif "johnson" in string:
        vaccines["janssen"] = True

    return vaccines


def article_to_location(html_article):
    """convert the html article entry in something closer to the normalized format."""

    name = html_article.find("h3")
    if name:
        name = name.get_text()

    identifier = location_id_from_name(name)
    vaccines = None

    transport = {"walk": None, "drive": None}
    phone_number = None
    address_l1, address_l2 = None, None
    register_link = None
    secondary_vax_str = None
    secondary_vaccines = None
    notes = None

    info_row = html_article.find("div", class_="info row")
    if info_row:
        date = info_row.find("div", class_="date")
        if date:
            date_paragraphs = date.find_all("p")
            for p in date_paragraphs:
                ptext = p.get_text().lower()
                # this is the vaccine type offered
                if ptext.startswith("vaccine"):
                    vaccines = detect_vaccine_types(ptext)

                # this may contain "walk in and drive-thru"
                else:
                    if "walk-in" in ptext:
                        transport["walk"] = True
                    elif "drive-thru" in ptext:
                        transport["drive"] = True

            secondary = date.find(class_="secondary")
            if secondary:
                ps = secondary.find_all("p")
                if len(ps) > 0:
                    secondary_vax_str = ps[-1].text.lower()
                    secondary_vaccines = detect_vaccine_types(secondary_vax_str)

        where = info_row.find("div", class_="where")
        if where:
            location = where.find(class_="location")
            address = location.find("p").get_text().strip()
            addressparts = address.split("\n")
            addressparts = [a.strip() for a in addressparts]
            if len(addressparts) == 3:
                address_l1 = addressparts[1]
                address_l2 = addressparts[2]
            else:
                address_l2 = ", ".join(addressparts)

            directions_link = where.find("a", text="Get Directions").get("href")

            call_to_schedule = where.find("span", class_="call")
            if call_to_schedule:
                number = call_to_schedule.find_next_sibling("a")

                phone_number = number.get_text()

            register_a = where.find("a", text="Register Online")
            if register_a:
                register_link = register_a.get("href")

    notes_html = html_article.find("h4")
    if notes_html:
        notes = notes_html.get_text()

    return {
        "id": identifier,
        "name": name,
        "vaccines_dose_1": vaccines,
        "vaccines_dose_2": secondary_vaccines,
        "transport": transport,
        "street_address": address_l1.strip() if address_l1 else "",
        "location": address_l2.strip() if address_l2 else "",
        "directions_link": directions_link or "",
        "phone_number": phone_number or "",
        "register_at": register_link or "",
        "notes:": notes,
    }


def parse_ladph_html(file_contents):
    """
    This parses the HTML into ndjson
    """
    soup = BeautifulSoup(file_contents, "html.parser")

    return (
        article_to_location(html_article) for html_article in soup.select("article")
    )


output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

html_filepaths = input_dir.glob("*.html")

for in_filepath in html_filepaths:
    with in_filepath.open() as fin:
        sites = parse_ladph_html(fin.read())

    filename = in_filepath.name.split(".", maxsplit=1)[0]
    out_filepath = output_dir / f"{filename}.parsed.ndjson"

    with out_filepath.open("w") as fout:
        for site in sites:
            json.dump(site, fout)
            fout.write("\n")
