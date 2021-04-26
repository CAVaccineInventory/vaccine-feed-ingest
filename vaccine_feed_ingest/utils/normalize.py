"""
Various tricks for matching source locations to product locations from VIAL
"""
import re
from typing import Optional, Tuple


def provider_id_from_name(name: str) -> Optional[Tuple[str, str]]:
    """ Generate provider ids for retail pharmacies (riteaid:123) """

    m = re.search(r"RITE AID PHARMACY (\d+)", name, re.I)
    if m:
        return "rite_aid", str(int(m.group(1)))
    m = re.search(r"Walgreens (?:Specialty )?(?:Pharmacy )?#(\d+)", name, re.I)
    if m:
        return "walgreens", str(int(m.group(1)))
    m = re.search(r"Safeway (?:PHARMACY )?\s?#?(\d+)", name, re.I)
    if m:
        return "safeway", str(int(m.group(1)))
    m = re.search(r"VONS PHARMACY #(\d+)", name, re.I)
    if m:
        return "vons", str(int(m.group(1)))
    m = re.search(r"SAMS PHARMACY 10-(\d+)", name, re.I)
    if m:
        return "sams", str(int(m.group(1)))
    m = re.search(r"SAV-?ON PHARMACY #\s?(\d+)", name, re.I)
    if m:
        # These are albertsons locations
        return "albertsons", str(int(m.group(1)))
    m = re.search(r"PAVILIONS PHARMACY #(\d+)", name, re.I)
    if m:
        return "pavilions", str(int(m.group(1)))
    m = re.search(r"WALMART PHARMACY 10-(\d+)", name, re.I)
    if m:
        return "walmart", str(int(m.group(1)))
    m = re.search(r"CVS\s(?:STORE)?(?:PHARMACY)?(?:, INC)?.?\s?#?(\d+)", name, re.I)
    if m:
        return "cvs", str(int(m.group(1)))

    return None
