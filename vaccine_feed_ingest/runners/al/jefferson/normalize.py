#!/usr/bin/env python

import json
import pathlib
import re
import sys
from hashlib import md5
from typing import Dict, List, Optional, Text, Tuple

from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.log import getLogger
from vaccine_feed_ingest.utils.normalize import (
    normalize_phone,
    normalize_url,
    normalize_zip,
)

logger = getLogger(__file__)

# Regexes to match different pieces of vaccine site info.
_STREET_ADDRESS_REGEX_STRING = r"(\d+\s+[A-Za-z0-9# .,-]+)"
_STREET_ADDRESS_REGEX = re.compile(r"^" + _STREET_ADDRESS_REGEX_STRING + r"$")
# Handle AL/Al, and variation in spacing and commas.
_CITY_STATE_ZIP_REGEX_STRING = r"([A-Za-z0-9 -]+)\s*,?\s*AL,?\s*([\d-]+)"
_CITY_STATE_ZIP_REGEX = re.compile(
    r"^" + _CITY_STATE_ZIP_REGEX_STRING + r"$", re.IGNORECASE
)
_PHONE_NUMBER_REGEX = re.compile(r"\(?\d+\)?\s*\d+[ -]?\d+")
# An address in a single string: street address, city, state, zip
_COMBINED_ADDRESS_REGEX = re.compile(
    r"^" + _STREET_ADDRESS_REGEX_STRING + r",\s+" + _CITY_STATE_ZIP_REGEX_STRING + r"$",
    re.IGNORECASE,
)
_DROP_IN_REGEX = re.compile(r"No appointments? necessary.*")
_APPOINTMENTS_REGEX = re.compile(r"Make an appointment here.*")
# Regex matching the names of entries to be ignored entirely.
_IGNORE_PREFIXES = [
    r"Alabama Department of Public Health \(ADPH\)",
    r"Centers for Disease Control \(CDC\)",
    # Assume a site with this line is covered by another entry
    # titled "Make an appointment"
    r"Find a location near you",
]
_IGNORE_REGEX = re.compile(r"(" + "|".join(_IGNORE_PREFIXES) + r"):?")
# Regex matching text that should not be used as the site name,
# although we want to include the site itself.
_IGNORE_NAME_REGEX = re.compile(r".*Locations to be determined.*")


def _make_placeholder_location(entry: dict) -> schema.NormalizedLocation:
    """Returns a normalized location with a placeholder ID,
    the given `entry` as source data, and all other fields empty."""
    source = _make_placeholder_source(entry)
    return schema.NormalizedLocation(id=_make_site_id(source), source=source)


def _add_id(site: schema.NormalizedLocation) -> None:
    """Generates source and site IDs for the given `site` object
    and updates the object in place.
    """
    # We don't have a stable site ID or name from the source document,
    # so generate one ID by hashing whatever name and address info we do have.
    # These are likely to be more stable than the phone or website info.
    # Avoid using the `page` and `provider` numbers from `entry`,
    # because those are sensitive to layout changes in the source document.
    candidate_data: List[Optional[Text]] = list(
        filter(
            None,
            [
                site.name,
                getattr(site.address, "street1", None),
                getattr(site.address, "city", None),
                getattr(site.address, "state", None),
                getattr(site.address, "zip", None),
            ],
        )
    )
    # Fall back to website or phone info if we don't have concrete name or location info.
    if not candidate_data:
        candidate_data.extend([c.website or c.phone for c in site.contact])

    site.source.id = _md5_hash(candidate_data)
    site_id = _make_site_id(site.source)
    logger.debug("Site ID: %s", site_id)
    site.id = site_id


_URL_HOST_TO_PROVIDER: Dict[Text, schema.VaccineProvider] = {
    "www.cvs.com": schema.VaccineProvider.CVS,
    "www.samsclub.com": schema.VaccineProvider.SAMS,
    "www.walmart.com": schema.VaccineProvider.WALMART,
    "www.winndixie.com": schema.VaccineProvider.WINN_DIXIE,
}


def _lookup_provider(website: schema.Contact) -> Optional[schema.Organization]:
    """Gets the vaccine provider for the given website, if known."""
    url = website.website
    provider = _URL_HOST_TO_PROVIDER.get(url.host, None) if url else None
    return schema.Organization(id=provider) if provider else None


def _add_website_and_provider(site: schema.NormalizedLocation, entry: dict) -> None:
    """Adds website and provider information from `entry`, if any,
    to the given `site` object."""
    # Create a fresh object each time, though many sites may have the same website.
    website = _make_website_contact(entry["link"])
    if website is not None:
        site.contact = site.contact or []
        site.contact.append(website)
        # Try to work out well-known providers from the URL.
        site.parent_organization = _lookup_provider(website)


def _finalize_site(site: schema.NormalizedLocation, entry: dict) -> None:
    """Adds website, provider, and ID information to the given `site`."""
    # Add the website and provider name, if we have it.
    _add_website_and_provider(site, entry)
    # Generate real IDs, now we have all the site information.
    _add_id(site)


def _get_combined_address(detail: Text) -> Optional[Tuple[Text, Text, Text]]:
    """Gets a tuple (street address, city, zip) from `detail`, if it is a
    complete single-line address. Otherwise returns `None`."""
    if (combined_match := _COMBINED_ADDRESS_REGEX.match(detail)) is not None:
        [street_address, city, zip] = combined_match.groups()[0:3]
        logger.debug(
            "One-line address '%s' split into address: '%s' city: '%s' zip: '%s'",
            detail,
            street_address,
            city,
            zip,
        )
        return street_address, city, zip
    return None


def _is_street_address(detail: Text) -> bool:
    if _STREET_ADDRESS_REGEX.match(detail) is not None:
        logger.debug("Street address: %s", detail)
        return True
    return False


def _get_city_zip(detail: Text) -> Optional[Tuple[Text, Text]]:
    """Gets a tuple `(city, zip)` from `detail`, if it contains the
    city+state+zip component of an Alabama address. Otherwise returns `None`."""
    if (city_state_zip_match := _CITY_STATE_ZIP_REGEX.match(detail)) is not None:
        # Assume the state is always AL
        [city, zip] = city_state_zip_match.groups()[0:2]
        logger.debug("City, state, zip: %s -> city %s, zip %s", detail, city, zip)
        return city, zip
    return None


def _is_phone(detail: Text) -> bool:
    if _PHONE_NUMBER_REGEX.match(detail) is not None:
        logger.debug("Phone: %s", detail)
        return True
    return False


def _is_drop_in(detail: Text) -> bool:
    if _DROP_IN_REGEX.match(detail) is not None:
        logger.debug("Availability = drop in: %s", detail)
        return True
    return False


def _is_appointments(detail: Text) -> bool:
    if _APPOINTMENTS_REGEX.match(detail) is not None:
        logger.debug("Availability = appt: %s", detail)
        return True
    return False


def _should_ignore(detail: Text) -> bool:
    """Whether the presence of `detail` indicates a generic parsed entry
    that is unlikely to be a vaccine site, and should be ignored entirely."""
    if _IGNORE_REGEX.match(detail) is not None:
        logger.debug("Ignoring generic entry: %s", detail)
        return True
    return False


class SiteBuilder:
    """
    Stateful builder to create a list of normalized vaccine sites
    for a single entry from the parse step.
    This is necessary because a single parsed entry may actually hold
    information about multiple vaccine sites for the same provider.

    Maintains a list of sites seen so far,
    and a current site to which incoming details are added.
    If incoming details are already present on the current site,
    the builder will start a new site to record those details.
    """

    _sites: List[schema.NormalizedLocation]
    _entry: dict
    _current_site: schema.NormalizedLocation

    def __init__(self, entry: dict) -> None:
        super().__init__()
        self._sites = []
        self._entry = entry
        self.fresh_site()

    def fresh_site(self) -> None:
        """Resets the current site to a fresh placeholder site."""
        logger.debug("Starting a fresh site")
        self._current_site = _make_placeholder_location(self._entry)

    def next_site(self) -> None:
        """Saves the current site and starts a fresh placeholder site."""
        logger.debug("Recording current site: %s", self._current_site)
        self._sites.append(self._current_site)
        self.fresh_site()

    def build_sites(self) -> List[schema.NormalizedLocation]:
        """Returns the final list of normalized sites from this builder."""
        # Include the last processed site.
        if self._current_site_has_info():
            self._sites.append(self._current_site)
        for site in self._sites:
            _finalize_site(site, self._entry)
        return self._sites

    def add_address_details(
        self,
        street_address: Optional[Text] = None,
        city: Optional[Text] = None,
        zip: Optional[Text] = None,
    ) -> None:
        """Adds the given address information to the current site.
        If the current site already has one of the provided fields,
        then starts a fresh site before recording the information.
        """
        # Start a new site if necessary.
        address = self._current_site.address
        if address and (
            (street_address and address.street1)
            or (city and address.city)
            or (zip and address.zip)
        ):
            self.next_site()

        # Create an Address object.
        site = self._current_site
        site.address = site.address or schema.Address(state="AL")
        # Add the given details.
        if street_address is not None:
            site.address.street1 = street_address
        if city is not None:
            site.address.city = city
        if zip is not None:
            site.address.zip = normalize_zip(zip)

    def add_generic_detail(self, detail: Text) -> None:
        """Adds the given generic detail to the current site,
        as the site name if suitable, or an additional note otherwise."""
        if self._current_site.name or _IGNORE_NAME_REGEX.match(detail) is not None:
            self.add_note(detail)
        else:
            self.add_name(detail)

    def _current_site_has_info(self) -> bool:
        """Whether the current site object has non-trivial information."""
        site = self._current_site
        return site.address or site.availability or site.name or site.contact

    def add_name(self, detail: Text) -> None:
        """Adds the given name to the current site."""
        logger.debug("Site name: %s", detail)
        self._current_site.name = detail

    def add_note(self, note: Text) -> None:
        """Adds the given note to the current site."""
        logger.debug("Additional note: %s", note)
        site = self._current_site
        site.notes = site.notes or []
        site.notes.append(note)

    def add_availability(
        self,
        drop_in: Optional[bool] = None,
        appointments: Optional[bool] = None,
    ) -> None:
        """Adds the given availability information to the current site."""
        site = self._current_site
        site.availability = site.availability or schema.Availability()
        if drop_in is not None:
            site.availability.drop_in = drop_in
        if appointments is not None:
            site.availability.appointments = appointments

    def add_phone(self, phone: Text) -> None:
        """Adds the given phone number to the current site."""
        # It's ok to have multiple phone numbers,
        # so no need to start fresh if we have a phone number already.
        site = self._current_site
        site.contact = site.contact or []
        site.contact.extend(normalize_phone(phone, contact_type="booking"))


def normalize(entry: dict) -> List[schema.NormalizedLocation]:
    """Gets a list of normalized vaccine site objects from a single parsed JSON entry."""
    details: List[Text] = entry.get("details", [])
    # The details list can be in one of the following forms,
    # and may contain info about multiple sites:
    # [combined address, optional phone]+
    # [optional name, street address, city state zip, optional phone]+
    # The parsed JSON has relatively little information about
    # provider names, because these are images in the original document.

    # Process each detail, building up a list of sites.
    site_builder = SiteBuilder(entry)
    for detail in details:
        # Trim whitespace and commas
        detail = detail.strip(" \t\n\r,")
        # Might be the entire address
        if combined_address := _get_combined_address(detail):
            street_address, city, zip = combined_address
            site_builder.add_address_details(
                street_address=street_address, city=city, zip=zip
            )
        # Or one component of site info
        elif _is_street_address(detail):
            site_builder.add_address_details(street_address=detail)
        elif city_zip := _get_city_zip(detail):
            # Assume the state is always AL
            [city, zip] = city_zip
            site_builder.add_address_details(city=city, zip=zip)
        elif _is_phone(detail):
            site_builder.add_phone(detail)
        elif _is_drop_in(detail):
            site_builder.add_availability(drop_in=True)
        elif _is_appointments(detail):
            site_builder.add_availability(appointments=True)
        elif _should_ignore(detail):
            # Ignore these entries entirely.
            # These are usually the Dept of Public Health or CDC website links,
            # or site info that is already captured in a different site.
            site_builder.fresh_site()
        else:
            # Either the site name or an extra note.
            site_builder.add_generic_detail(detail)

    return site_builder.build_sites()


_SOURCE_NAME = "al_jefferson"


def _make_placeholder_source(entry: dict) -> schema.Source:
    """Returns a `schema.Source` object referring to the original `entry` data,
    with placeholder ID information."""
    return schema.Source(
        source=_SOURCE_NAME,
        id="PLACEHOLDER",
        fetched_from_uri=entry.get("fetched_from_uri", None),
        fetched_at=None,
        published_at=entry.get("published_at", None),
        data=entry,
    )


def _make_site_id(source: schema.Source) -> Text:
    """Returns a site ID compatible with `source`, according to the schema validation rules."""
    return f"{source.source}:{source.id}"


def _md5_hash(inputs: List[Optional[Text]]) -> Text:
    """Generates an md5 checksum from the truthy inputs."""
    return md5("".join(filter(None, inputs)).encode("utf-8")).hexdigest()


def _make_website_contact(url: Optional[Text]) -> Optional[schema.Contact]:
    """Returns a `schema.Contact` object for the given booking URL, if any."""
    normalized_url = normalize_url(url)
    if normalized_url:
        return schema.Contact(contact_type="booking", website=normalized_url)
    return None


def main():
    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])

    json_filepaths = input_dir.glob("*.ndjson")

    for in_filepath in json_filepaths:
        filename = in_filepath.name.split(".", maxsplit=1)[0]
        out_filepath = output_dir / f"{filename}.normalized.ndjson"
        logger.info(
            "Normalizing: %s => %s",
            in_filepath,
            out_filepath,
        )

        with in_filepath.open() as fin:
            with out_filepath.open("w") as fout:
                locations: Dict[Text, schema.NormalizedLocation] = dict()
                for site_json in fin:
                    parsed_site = json.loads(site_json)
                    # One line of parsed json may describe
                    # multiple vaccine sites after normalization.
                    normalized_sites = normalize(parsed_site)
                    if not normalized_sites:
                        # These entries are usually Dept of Public Health
                        # or CDC website links, not info about vaccine sites.
                        logger.info("Entry has no vaccine site info: %s", parsed_site)
                    for normalized_site in normalized_sites:
                        # Handle duplicates. These may come from the
                        # English and Spanish sections of the same document,
                        # which mostly list the same sites.
                        existing = locations.get(normalized_site.id, None)
                        if existing:
                            # If we've seen this site before, make sure the content is identical.
                            # Don't compare the `source` fields:
                            # they will always have different document locations.
                            existing_data = existing.dict(
                                exclude_none=True,
                                exclude_unset=True,
                                exclude={"source"},
                            )
                            new_data = normalized_site.dict(
                                exclude_none=True,
                                exclude_unset=True,
                                exclude={"source"},
                            )
                            if existing_data != new_data:
                                logger.warning(
                                    "Found different locations with the ID %s, ignoring the second:\n%s\n%s",
                                    normalized_site.id,
                                    existing_data,
                                    new_data,
                                )
                            else:
                                # Not a problem, but useful to know while developing.
                                logger.debug(
                                    "Found duplicates of site with ID %s, will record only one",
                                    normalized_site.id,
                                )
                        else:
                            locations[normalized_site.id] = normalized_site
                            json.dump(normalized_site.dict(exclude_unset=True), fout)
                            fout.write("\n")


if __name__ == "__main__":
    main()
