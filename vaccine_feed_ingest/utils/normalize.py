"""
Various tricks for matching source locations to product locations from VIAL
"""
import re
from typing import Optional, Tuple

import url_normalize
from vaccine_feed_ingest_schema.location import VaccineProvider

# Add to this list in alphabetical order
VACCINE_PROVIDER_REGEXES = {
    VaccineProvider.ACME: [
        re.compile(r"ACME PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.ALBERTSONS: [
        re.compile(r"SAV-?ON PHARMACY #\s?(\d+)", re.I),
        re.compile(r"ALBERTSONS(?: MARKET)? PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.BIG_Y: [
        re.compile(r"BIG Y PHARMACY(?: #\d+ Rx)? #(\d+)", re.I),
    ],
    VaccineProvider.BROOKSHIRE: [
        re.compile(r"BROOKSHIRE PHARMACY #\d+ #(\d+)", re.I),
    ],
    VaccineProvider.COSTCO: [
        re.compile(r"COSTCO(?: MARKET)? PHARMACY #\s*(\d+)", re.I),
        re.compile(r"COSTCO WHOLESALE CORPORATION #(\d+)", re.I),
    ],
    VaccineProvider.CUB: [
        re.compile(r"CUB PHARMACY #\d+ #(\d+)", re.I),
    ],
    VaccineProvider.CVS: [
        re.compile(r"CVS\s(?:STORE)?(?:PHARMACY)?(?:, INC.?)?\s?#?(\d+)", re.I),
    ],
    VaccineProvider.DILLONS: [
        re.compile(r"DILLON\'S PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.DRUGCO: [
        re.compile(r"DRUGCO DISCOUNT PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.FAMILY_FARE: [
        re.compile(r"FAMILY\s+FARE\s+PHARMACY\s+#?\d+\s+#(\d+)", re.I),
    ],
    VaccineProvider.FOOD_CITY: [
        re.compile(r"FOOD CITY PHARMACY #\d+ #(\d+)", re.I),
    ],
    VaccineProvider.FOOD_LION: [
        re.compile(r"FOOD LION #(\d+)", re.I),
    ],
    VaccineProvider.FRED_MEYER: [
        re.compile(r"FRED MEYER(?: PHARMACY)? #(\d+)", re.I),
    ],
    VaccineProvider.FRYS: [
        re.compile(r"FRY\'S FOOD AND DRUG #(\d+)", re.I),
    ],
    VaccineProvider.GENOA: [
        re.compile(r"GENOA HEALTHCARE (\d+) \(", re.I),
        re.compile(r"GENOA HEALTHCARE LLC #(\d+)", re.I),
    ],
    VaccineProvider.GIANT: [
        re.compile(r"GIANT #(\d+)", re.I),
    ],
    VaccineProvider.GIANT_EAGLE: [
        re.compile(r"GIANT EAGLE PHARMACY #\d+ #G(\d+)", re.I),
    ],
    VaccineProvider.GIANT_FOOD: [
        re.compile(r"GIANT FOOD #(\d+)", re.I),
    ],
    VaccineProvider.HEB: [
        re.compile(r"H-E-B #(\d+)", re.I),
    ],
    VaccineProvider.HAGGEN: [
        re.compile(r"HAGGEN PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.HANNAFORD: [
        re.compile(r"HANNAFORD #(\d+)", re.I),
    ],
    VaccineProvider.HARMONS: [
        re.compile(r"HARMONS PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.HARPS: [
        re.compile(r"HARPS PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.HARRIS_TEETER: [
        re.compile(r"HARRIS TEETER PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.HARTIG: [
        re.compile(r"HARTIG DRUG CO #?\d+ #(\d+)", re.I),
    ],
    VaccineProvider.HOMELAND: [
        re.compile(r"HOMELAND PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.HY_VEE: [
        re.compile(r"HY-VEE INC. #(\d+)", re.I),
    ],
    VaccineProvider.KAISER_HEALTH_PLAN: [
        re.compile(r"KAISER HEALTH PLAN \w+(?: \w+)? PHY (\d+)", re.I),
    ],
    VaccineProvider.KAISER_PERMANENTE: [
        re.compile(r"KAISER PERMANENTE PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.KING_SOOPERS: [
        re.compile(r"KING SOOPERS PHARMACY #?(\d+)", re.I),
    ],
    VaccineProvider.KROGER: [
        re.compile(r"KROGER PHARMACY #?(\d+)", re.I),
    ],
    VaccineProvider.INGLES: [
        re.compile(r"INGLES PHARMACY #\d+ #(\d+)", re.I),
    ],
    VaccineProvider.PAVILIONS: [
        re.compile(r"PAVILIONS PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.RITE_AID: [
        re.compile(r"RITE AID PHARMACY (\d+)", re.I),
    ],
    VaccineProvider.SAMS: [
        re.compile(r"SAM'?S PHARMACY (?:10-|#\s*)(\d+)", re.I),
    ],
    VaccineProvider.SAFEWAY: [
        re.compile(r"Safeway (?:PHARMACY )?\s?#?(\d+)", re.I),
    ],
    VaccineProvider.VONS: [
        re.compile(r"VONS PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.WALGREENS: [
        re.compile(r"Walgreens (?:Specialty )?(?:Pharmacy )?#(\d+)", re.I),
    ],
    VaccineProvider.WALMART: [
        re.compile(r"WALMART PHARMACY 10-(\d+)", re.I),
        re.compile(r"WALMART (?:INC,|PHARMACY) #(\d+)", re.I),
    ],
}


def provider_id_from_name(
    name: str,
) -> Optional[Tuple[VaccineProvider, str]]:
    """Generate provider ids for retail pharmacies (riteaid:123)"""

    for vaccine_provider, regexes in VACCINE_PROVIDER_REGEXES.items():
        for regex in regexes:
            m = regex.search(name)
            if m:
                return vaccine_provider, str(int(m.group(1)))

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
