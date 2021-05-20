"""
Various tricks for matching source locations to product locations from VIAL
"""
import hashlib
import re
from typing import List, Optional, Tuple

import orjson
import phonenumbers
import url_normalize
from vaccine_feed_ingest_schema import location
from vaccine_feed_ingest_schema.location import VaccineProvider

from .log import getLogger

logger = getLogger(__file__)


# Add to this list in alphabetical order
VACCINE_PROVIDER_REGEXES = {
    VaccineProvider.ACME: [
        re.compile(r"ACME PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.ALBERTSONS: [
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
    VaccineProvider.HEB: [
        re.compile(r"H-E-B #(\d+)", re.I),
    ],
    VaccineProvider.HOMELAND: [
        re.compile(r"HOMELAND PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.HY_VEE: [
        re.compile(r"HY-VEE INC. #(\d+)", re.I),
    ],
    VaccineProvider.INGLES: [
        re.compile(r"INGLES PHARMACY #\d+ #(\d+)", re.I),
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
    VaccineProvider.LITTLE_CLINIC: [
        re.compile(r"THE LITTLE CLINIC #(\d+)", re.I),
    ],
    VaccineProvider.MARIANOS: [
        re.compile(r"MARIANO\'S PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.OSCO: [
        re.compile(r"OSCO (?:DRUG|PHARMACY) #(\d+)", re.I),
    ],
    VaccineProvider.MARKET_STREET: [
        re.compile(r"MARKET STREET PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.MEDICAP: [
        re.compile(r"MEDICAP PHARMACY #\d+ #(\d+)", re.I),
    ],
    VaccineProvider.MEIJER: [
        re.compile(r"MEIJER #(\d+)", re.I),
    ],
    VaccineProvider.PAVILIONS: [
        re.compile(r"PAVILIONS PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.PICK_N_SAVE: [
        re.compile(r"PICK N SAVE PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.PRICE_CHOPPER: [
        re.compile(r"PRICE CHOPPER PHARMACY #?\d+ #(?:MS)?(\d+)", re.I),
    ],
    VaccineProvider.PUBLIX: [
        re.compile(r"PUBLIX SUPER MARKETS INC\. #(\d+)", re.I),
    ],
    VaccineProvider.QFC: [
        re.compile(r"QFC PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.RALEYS: [
        re.compile(r"RALEY\'S PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.RITE_AID: [
        re.compile(r"RITE AID (?:PHARMACY |#RA)(\d+)", re.I),
    ],
    VaccineProvider.SAMS: [
        re.compile(r"SAM'?S PHARMACY (?:10-|#\s*)(\d+)", re.I),
        re.compile(r"SAMS CLUB (?:#\d+\-)?(\d+)", re.I),
    ],
    VaccineProvider.SAFEWAY: [
        re.compile(r"Safeway (?:PHARMACY )?\s?#?(\d+)", re.I),
    ],
    VaccineProvider.SAV_ON: [
        re.compile(r"SAV-?ON PHARMACY #\s?(\d+)", re.I),
    ],
    VaccineProvider.SHOP_RITE: [
        re.compile(r"SHOPRITE PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.SMITHS: [
        re.compile(r"SMITH\'S PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.STOP_AND_SHOP: [
        re.compile(r"STOP \& SHOP #(\d+)", re.I),
    ],
    VaccineProvider.TOM_THUMB: [
        re.compile(r"TOM THUMB PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.THRIFTY: [
        re.compile(r"THRIFTY DRUG STORES INC #(\d+)", re.I),
    ],
    VaccineProvider.VONS: [
        re.compile(r"VONS PHARMACY #(\d+)", re.I),
    ],
    VaccineProvider.WALGREENS: [
        re.compile(r"Walgreens (?:Specialty )?(?:Pharmacy )?#(\d+)", re.I),
        re.compile(r"Walgreens Co\. #(\d+)", re.I),
    ],
    VaccineProvider.WALMART: [
        re.compile(r"WALMART INC #10-(\d+)", re.I),
        re.compile(r"WALMART PHARMACY 10-(\d+)", re.I),
        re.compile(r"WALMART (?:INC,|PHARMACY) #(\d+)", re.I),
    ],
    VaccineProvider.WEIS: [
        re.compile(r"WEIS PHARMACY #\d+ #(\d+)", re.I),
    ],
    VaccineProvider.WINN_DIXIE: [
        re.compile(r"WINN-DIXIE #(\d+)", re.I),
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


def normalize_phone(
    phone: Optional[str], contact_type: Optional[str] = None
) -> List[location.Contact]:
    if phone is None:
        return []

    # Canonicalize various terms; lowercase to simplify.
    phone = str(phone)  # nc/myspot_gov has some entries that are bare numbers.
    phone = phone.lower()
    phone = phone.replace(" option ", " ext. ")
    phone = phone.replace(" press #", " ext. ")
    phone = phone.replace(" press ", " ext. ")

    contacts = []
    for match in phonenumbers.PhoneNumberMatcher(phone, "US"):
        contacts.append(
            location.Contact(
                contact_type=contact_type,
                phone=phonenumbers.format_number(
                    match.number, phonenumbers.PhoneNumberFormat.NATIONAL
                ),
            )
        )
    return contacts


def calculate_content_hash(loc: location.NormalizedLocation) -> str:
    """Calculate a hash from the normalized content of a location without source data"""
    loc_dict = loc.dict(exclude_none=True, exclude={"source"})
    loc_json = orjson.dumps(loc_dict, option=orjson.OPT_SORT_KEYS)
    return hashlib.md5(loc_json).hexdigest()
