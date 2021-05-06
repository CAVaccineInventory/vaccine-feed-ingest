import re
from typing import Optional

import jellyfish
import us
from vaccine_feed_ingest_schema import location

from .log import getLogger
from .normalize import normalize_address
from .parse import parse_address

logger = getLogger(__file__)


def is_address_similar(
    source: location.NormalizedLocation,
    candidate: dict,
    threshold: float = 0.95,
) -> Optional[bool]:
    """Compare address for similarity

    - True if similarity is above threashold
    - False if similarity is below threashold
    - None if couldn't compare addresses
    """
    source_address = normalize_address(source.address)
    if not source_address:
        return None

    if not candidate["properties"].get("full_address"):
        return None

    candidate_address = parse_address(candidate["properties"]["full_address"])
    if not candidate_address:
        return None

    # Fill in missing parts of the candidate address from the address parts
    if not candidate_address.city:
        candidate_address.city = candidate["properties"].get("city")

    if not candidate_address.state:
        candidate_address.state = candidate["properties"].get("state")

    if not candidate_address.zip:
        candidate_address.zip = candidate["properties"].get("zip_code")

    candidate_address = normalize_address(candidate_address)
    if not candidate_address:
        return None

    # States must exist to compare
    if not source_address.state or not candidate_address.state:
        return None

    # Cities must exist to compare
    if not source_address.city or not candidate_address.city:
        return None

    # Streets must exist to compare
    if not source_address.street1 or not candidate_address.street1:
        return None

    # States must exactly match
    src_state = us.states.lookup(source_address.state)
    cand_state = us.states.lookup(candidate_address.state)

    if src_state != cand_state:
        return False

    # Cities must have similar names within threashold
    src_city = source_address.city.lower()
    cand_city = candidate_address.city.lower()
    if jellyfish.jaro_winkler(src_city, cand_city) < threshold:
        return False

    # If zip code exists then zip codes must match the first 5 digits
    if source_address.zip and candidate_address.zip:
        src_zip = source_address.zip[:5]
        cand_zip = candidate_address.zip[:5]

        if src_zip != cand_zip:
            return False

    # Street address be similar within threashold
    src_street_parts = [source_address.street1]
    if source_address.street2:
        src_street_parts.append(source_address.street2)
    src_street = " ".join(src_street_parts)

    cand_street_parts = [candidate_address.street1]
    if candidate_address.street2:
        cand_street_parts.append(candidate_address.street2)
    cand_street = " ".join(cand_street_parts)

    if jellyfish.jaro_winkler(src_street, cand_street) < threshold:
        return False

    # If it made it though the gauntlet then it is similar eough to be a match
    return True


def get_full_address(address: Optional[location.Address]) -> str:
    if address is None:
        return ""
    if address.street2:
        return "{}\n{}\n{}, {} {}".format(
            address.street1, address.street2, address.city, address.state, address.zip
        )
    else:
        return "{}\n{}, {} {}".format(
            address.street1, address.city, address.state, address.zip
        )


def canonicalize_address(address: str) -> str:
    """
    >>> canonicalize("460 W San Ysidro Blvd, San Ysidro, CA 92173, United States")
    '460 west san ysidro boulevard, san ysidro, ca 92173'
    >>> canonicalize("1208 WEST REDONDO BEACH BOULEVARD, GARDENA, CA 90247")
    '1208 west redondo beach boulevard, gardena, ca 90247'
    >>> canonicalize("1208 West Redondo Beach Blvd., Gardena, CA 90247")
    '1208 west redondo beach boulevard, gardena, ca 90247'
    >>> canonicalize("555 E. Valley Pkwy, Escondido, CA 92025")
    '555 east valley parkway, escondido, ca 92025'
    >>> canonicalize("500 OLD RIVER RD STE 125, BAKERSFIELD, CA 93311")
    '500 old river road suite 125, bakersfield, ca 93311'
    >>> canonicalize("2419 EAST AVENUE  SOUTH, PALMDALE, CA 93550")
    '2419 east avenue south, palmdale, ca 93550'
    >>> canonicalize("2419 East Avenue S, Palmdale, CA 93550")
    '2419 east avenue south, palmdale, ca 93550'
    >>> canonicalize("7239 WOODMAN AVENUE, VAN NUYS, CA 91405")
    '7239 woodman avenue, van nuys, ca 91405'
    >>> canonicalize("7239 Woodman Ave, Van Nuys, CA 91405")
    '7239 woodman avenue, van nuys, ca 91405'
    >>> canonicalize("10823 ZELZAH AVENUE BUILDING D, GRANADA HILLS, CA 91344")
    '10823 zelzah avenue building d, granada hills, ca 91344'
    >>> canonicalize("10823 Zelzah Avenue Bldg D, Granada Hills, CA 91344")
    '10823 zelzah avenue building d, granada hills, ca 91344'
    >>> canonicalize("23 PENINSULA CENTER, ROLLING HILLS ESTATES, CA 90274")
    '23 peninsula center, rolling hills estates, ca 90274'
    >>> canonicalize("23 Peninsula Center, Rolling Hills Ests, CA 90274")
    '23 peninsula center, rolling hills estates, ca 90274'
    >>> canonicalize("2352 Arrow Hwy (Gate 15) , Pomona, CA 91768")
    '2352 arrow hwy (gate 15), pomona, ca 91768'
    >>> canonicalize("11798 Foothill Blvd., , Lake View Terrace, CA 91342")
    '11798 foothill boulevard, lake view terrace, ca 91342'
    >>> canonicalize('808 W. 58th St. \\nLos Angeles, CA 90037')
    '808 west 58th street, los angeles, ca 90037'
    >>> canonicalize("45104 10th St W\\nLancaster, CA 93534")
    '45104 10th street west, lancaster, ca 93534'
    >>> canonicalize("133 W Rte 66, Glendora, CA 91740")
    '133 west route 66, glendora, ca 91740'
    >>> canonicalize("3410 W THIRD ST, LOS ANGELES, CA 90020")
    '3410 west 3rd street, los angeles, ca 90020'
    """

    a = address.lower().strip()
    if a.endswith(", united states"):
        a = a[: -len(", united states")]

    a = re.sub(r"([^,])\n+", r"\1, ", a)  # newline instead of comma
    a = re.sub(r",\s+,", ", ", a)  # repeated comma
    a = re.sub(r"\s+, ", ", ", a)  # extra space around comma

    a = re.sub(r" e\.?(\W)? ", r" east\1 ", a, re.I)
    a = re.sub(r" w\.?(\W)? ", r" west\1 ", a)
    a = re.sub(r" n\.?(\W)? ", r" north\1 ", a)
    a = re.sub(r" s\.?(\W)? ", r" south\1 ", a)

    a = re.sub(r" ave\.?(\W)", r" avenue\1", a)
    a = re.sub(r" blvd\.?(\W)", r" boulevard\1", a)
    a = re.sub(r" ctr\.?(\W)", r" center\1", a)
    a = re.sub(r" ests\.?(\W)", r" estates\1", a)
    a = re.sub(r" expy\.?(\W)", r" expressway\1", a)
    a = re.sub(r" hwy\.?(\W)", r" highway\1", a)
    a = re.sub(r" ln\.?(\W)", r" lane\1", a)
    a = re.sub(r" rd\.?(\W)", r" road\1", a)
    a = re.sub(r" pkwy\.?(\W)", r" parkway\1", a)
    a = re.sub(r" rte\.?(\W)", r" route\1", a)
    a = re.sub(r" ste\.?(\W)", r" suite\1", a)
    a = re.sub(r" st\.?(\W)", r" street\1", a)
    a = re.sub(r" wy\.?(\W)", r" way\1", a)

    a = re.sub(r"([a-z]) dr\.?(\W)", r"\1 drive\2", a)  # "drive" not "doctor"

    a = re.sub(r" bldg\.?(\W)", r" building\1", a)

    # Use numeric version of street names.
    # i.e. "1st" instead of "first"
    a = re.sub(r"(\W)first(\W)", r"\g<1>1st\g<2>", a)
    a = re.sub(r"(\W)second(\W)", r"\g<1>2nd\g<2>", a)
    a = re.sub(r"(\W)third(\W)", r"\g<1>3rd\g<2>", a)
    a = re.sub(r"(\W)fourth(\W)", r"\g<1>4th\g<2>", a)
    a = re.sub(r"(\W)fifth(\W)", r"\g<1>5th\g<2>", a)
    a = re.sub(r"(\W)sixth(\W)", r"\g<1>6th\g<2>", a)
    a = re.sub(r"(\W)seventh(\W)", r"\g<1>7th\g<2>", a)
    a = re.sub(r"(\W)eighth(\W)", r"\g<1>8th\g<2>", a)
    a = re.sub(r"(\W)ninth(\W)", r"\g<1>9th\g<2>", a)
    a = re.sub(r"(\W)tenth(\W)", r"\g<1>10th\g<2>", a)

    a = re.sub(r"\s+", " ", a)
    return a


def canonicalize_phone_number(phone_number: str) -> str:
    p = phone_number.strip()
    p = re.sub(r"^\+?1", "", p)  # remove leading 1/+1 if present
    p = re.sub(r"[^0-9]", "", p)  # remove all non-numeric characters
    return p
