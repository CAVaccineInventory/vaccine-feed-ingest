"""
Various tricks for matching source locations to product locations from VIAL
"""
import re
from typing import Optional, Tuple

import url_normalize
from vaccine_feed_ingest_schema.location import VaccineProvider

def organization_from_name(name: str) -> Optional[Tuple[str, str]]:
    """
    Generate parent organization for retail pharmacies (walgreens, Walgreens)

    We may want to merge this function with provider_id_from_name if we can get more clever regex's
    """

    m = re.search(r"(Walgreens)", name, re.I)
    if m:
        return m.group(1).lower(), m.group(1).title()

    m = re.search(r"(CVS)", name, re.I)
    if m:
        return m.group(1).lower(), m.group(1).upper()

    m = re.search(r"(Costco)", name, re.I)
    if m:
        return m.group(1).lower(), m.group(1).title()

    return None

def provider_id_from_name(
    name: str,
) -> Optional[Tuple[VaccineProvider, str]]:
    """Generate provider ids for retail pharmacies (riteaid:123)"""

    m = re.search(r"RITE AID PHARMACY (\d+)", name, re.I)
    if m:
        return VaccineProvider.RITE_AID, str(int(m.group(1)))
    m = re.search(r"Walgreens (?:Specialty )?(?:Pharmacy )?#(\d+)", name, re.I)
    if m:
        return VaccineProvider.WALGREENS, str(int(m.group(1)))
    m = re.search(r"Safeway (?:PHARMACY )?\s?#?(\d+)", name, re.I)
    if m:
        return VaccineProvider.SAFEWAY, str(int(m.group(1)))
    m = re.search(r"VONS PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.VONS, str(int(m.group(1)))
    m = re.search(r"SAM'?S PHARMACY (?:10-|#\s*)(\d+)", name, re.I)
    if m:
        return VaccineProvider.SAMS, str(int(m.group(1)))
    m = re.search(r"SAV-?ON PHARMACY #\s?(\d+)", name, re.I)
    if m:
        # These are albertsons locations
        return VaccineProvider.ALBERTSONS, str(int(m.group(1)))
    m = re.search(r"PAVILIONS PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.PAVILIONS, str(int(m.group(1)))
    m = re.search(r"WALMART PHARMACY 10-(\d+)", name, re.I)
    if m:
        return VaccineProvider.WALMART, str(int(m.group(1)))
    m = re.search(r"WALMART (?:INC,|PHARMACY) #(\d+)", name, re.I)
    if m:
        return VaccineProvider.WALMART, str(int(m.group(1)))
    m = re.search(r"CVS\s(?:STORE)?(?:PHARMACY)?(?:, INC.?)?\s?#?(\d+)", name, re.I)
    if m:
        return VaccineProvider.CVS, str(int(m.group(1)))
    m = re.search(r"ACME PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.ACME, str(int(m.group(1)))
    m = re.search(r"ALBERTSONS(?: MARKET)? PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.ALBERTSONS, str(int(m.group(1)))
    m = re.search(r"BIG Y PHARMACY(?: #\d+ Rx)? #(\d+)", name, re.I)
    if m:
        return VaccineProvider.BIG_Y, str(int(m.group(1)))
    m = re.search(r"BROOKSHIRE PHARMACY #\d+ #(\d+)", name, re.I)
    if m:
        return VaccineProvider.BROOKSHIRE, str(int(m.group(1)))
    m = re.search(r"COSTCO(?: MARKET)? PHARMACY #\s*(\d+)", name, re.I)
    if m:
        return VaccineProvider.COSTCO, str(int(m.group(1)))
    m = re.search(r"COSTCO WHOLESALE CORPORATION #(\d+)", name, re.I)
    if m:
        return VaccineProvider.COSTCO, str(int(m.group(1)))
    m = re.search(r"CUB PHARMACY #\d+ #(\d+)", name, re.I)
    if m:
        return VaccineProvider.CUB, str(int(m.group(1)))
    m = re.search(r"DILLON\'S PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.DILLONS, str(int(m.group(1)))
    m = re.search(r"DRUGCO DISCOUNT PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.DRUGCO, str(int(m.group(1)))
    m = re.search(r"FAMILY\s+FARE\s+PHARMACY\s+#?\d+\s+#(\d+)", name, re.I)
    if m:
        return VaccineProvider.FAMILY_FARE, str(int(m.group(1)))
    m = re.search(r"FOOD CITY PHARMACY #\d+ #(\d+)", name, re.I)
    if m:
        return VaccineProvider.FOOD_CITY, str(int(m.group(1)))
    m = re.search(r"FOOD LION #(\d+)", name, re.I)
    if m:
        return VaccineProvider.FOOD_LION, str(int(m.group(1)))
    m = re.search(r"FRED MEYER(?: PHARMACY)? #(\d+)", name, re.I)
    if m:
        return VaccineProvider.FRED_MEYER, str(int(m.group(1)))
    m = re.search(r"FRY\'S FOOD AND DRUG #(\d+)", name, re.I)
    if m:
        return VaccineProvider.FRYS, str(int(m.group(1)))
    m = re.search(r"GENOA HEALTHCARE (\d+) \(", name, re.I)
    if m:
        return VaccineProvider.GENOA, str(int(m.group(1)))
    m = re.search(r"GENOA HEALTHCARE LLC #(\d+)", name, re.I)
    if m:
        return VaccineProvider.GENOA, str(int(m.group(1)))
    m = re.search(r"GIANT #(\d+)", name, re.I)
    if m:
        return VaccineProvider.GIANT, str(int(m.group(1)))
    m = re.search(r"GIANT EAGLE PHARMACY #\d+ #G(\d+)", name, re.I)
    if m:
        return VaccineProvider.GIANT_EAGLE, str(int(m.group(1)))
    m = re.search(r"GIANT FOOD #(\d+)", name, re.I)
    if m:
        return VaccineProvider.GIANT_FOOD, str(int(m.group(1)))
    m = re.search(r"H-E-B #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HEB, str(int(m.group(1)))
    m = re.search(r"HAGGEN PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HAGGEN, str(int(m.group(1)))
    m = re.search(r"HANNAFORD #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HANNAFORD, str(int(m.group(1)))
    m = re.search(r"HARMONS PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HARMONS, str(int(m.group(1)))
    m = re.search(r"HARPS PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HARPS, str(int(m.group(1)))
    m = re.search(r"HARRIS TEETER PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HARRIS_TEETER, str(int(m.group(1)))
    m = re.search(r"HARTIG DRUG CO #?\d+ #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HARTIG, str(int(m.group(1)))
    m = re.search(r"HOMELAND PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HOMELAND, str(int(m.group(1)))
    m = re.search(r"HY-VEE INC. #(\d+)", name, re.I)
    if m:
        return VaccineProvider.HY_VEE, str(int(m.group(1)))
    m = re.search(r"KAISER HEALTH PLAN \w+(?: \w+)? PHY (\d+)", name, re.I)
    if m:
        return VaccineProvider.KAISER_HEALTH_PLAN, str(int(m.group(1)))
    m = re.search(r"KAISER PERMANENTE PHARMACY #(\d+)", name, re.I)
    if m:
        return VaccineProvider.KAISER_PERMANENTE, str(int(m.group(1)))
    m = re.search(r"KING SOOPERS PHARMACY #?(\d+)", name, re.I)
    if m:
        return VaccineProvider.KING_SOOPERS, str(int(m.group(1)))
    m = re.search(r"KROGER PHARMACY #?(\d+)", name, re.I)
    if m:
        return VaccineProvider.KROGER, str(int(m.group(1)))
    m = re.search(r"INGLES PHARMACY #\d+ #(\d+)", name, re.I)
    if m:
        return VaccineProvider.INGLES, str(int(m.group(1)))

    return None


ZIP_RE = re.compile(r"([0-9]{5})([0-9]{4})")


def normalize_zip(zipc: Optional[str]) -> Optional[str]:
    if zipc is not None:
        if ZIP_RE.match(zipc):
            zipc = ZIP_RE.sub(r"\1-\2", zipc)
        length = len(zipc)
        if length != 5 and length != 10:
            zipc = None

    return zipc


def normalize_url(url: Optional[str]) -> Optional[str]:
    if url is None:
        return url

    return url_normalize.url_normalize(url)
