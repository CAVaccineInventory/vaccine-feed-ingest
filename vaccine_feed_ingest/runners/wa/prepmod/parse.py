#!/usr/bin/env python

import json
import pathlib
import sys

import bs4

output_dir = pathlib.Path(sys.argv[1])
input_dir = pathlib.Path(sys.argv[2])

html_filepaths = input_dir.glob("*.html")

for in_filepath in html_filepaths:
    filename = in_filepath.name.split(".", maxsplit=1)[0]
    out_filepath = output_dir / f"{filename}.parsed.ndjson"
    with in_filepath.open("r") as fin:
        soup = bs4.BeautifulSoup(fin, "html.parser")

    sites = []
    for html_site in soup.find_all("div", class_="text-gray-800"):
        json_site = {}
        paras = html_site.find_all("p")
        if len(paras) < 3:
            continue
        json_site["name"] = "".join(paras[0].stripped_strings)
        json_site["address"] = "".join(paras[1].stripped_strings)
        for para in paras[2:]:
            strongs = para.find_all("strong")
            if strongs:
                label = "".join(strongs[0].stripped_strings).lower()
                if label[-1] == ":":
                    label = label[0:-1].strip()

                value_bits = []
                for d in para.contents:
                    if d == strongs[0]:
                        continue
                    elif isinstance(d, str):
                        value_bits.append(d.strip())
                    else:
                        text = "".join(d.stripped_strings)
                        value_bits.append(text)
                json_site[label] = "".join(value_bits)

        sites.append(json_site)

    with out_filepath.open("w") as fout:
        for site in sites:
            json.dump(site, fout)
            fout.write("\n")
