"""
Various tricks for matching source locations to product locations from VIAL
"""
import re
from typing import Optional, Tuple


def provider_id_from_name(name: str) -> Optional[Tuple[str, str]]:
    """ Generate provider ids for retail pharmacies (riteaid:123) """

    m = re.search(r"RITE AID PHARMACY (\d+)", name, re.I)
    if m:
        return "rite_aid", m.group(1)
    m = re.search(r"Walgreens (?:Specialty )?(?:Pharmacy )?#(\d+)", name, re.I)
    if m:
        return "walgreens", m.group(1)
    m = re.search(r"Safeway (?:PHARMACY )?\s?#?(\d+)", name, re.I)
    if m:
        return "safeway", m.group(1)
    m = re.search(r"VONS PHARMACY #(\d+)", name, re.I)
    if m:
        return "vons", m.group(1)
    m = re.search(r"SAMS PHARMACY 10-(\d+)", name, re.I)
    if m:
        return "sams", m.group(1)
    m = re.search(r"SAV-?ON PHARMACY #\s?(\d+)", name, re.I)
    if m:
        # These are albertsons locations
        return "albertsons", m.group(1)
    m = re.search(r"PAVILIONS PHARMACY #(\d+)", name, re.I)
    if m:
        return "pavilions", m.group(1)
    m = re.search(r"WALMART PHARMACY 10-(\d+)", name, re.I)
    if m:
        return "walmart", m.group(1)
    m = re.search(r"CVS\s(?:STORE)?(?:PHARMACY)?(?:, INC)?.?\s?#?(\d+)", name, re.I)
    if m:
        return "cvs", m.group(1)

    return None
