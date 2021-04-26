#!/usr/bin/env python3

from typing import List, Optional

from pydantic import BaseModel


class Address(BaseModel):
    """
    {
        "street1": str,
        "street2": str,
        "city": str,
        "state": str as state initial e.g. CA,
        "zip": str,
    },
    """

    street1: str
    street2: Optional[str]
    city: str
    state: str
    zip: str


class LatLng(BaseModel):
    """
    {
        "latitude": float,
        "longitude": float,
    },
    """

    latitude: float
    longitude: float


class Contact(BaseModel):
    """
    {
        "contact_type": str as contact type enum e.g. booking,
        "phone": str as (###) ###-###,
        "website": str,
        "email": str,
        "other": str,
    }
    """

    contact_type: Optional[str]
    phone: Optional[str]
    website: Optional[str]
    email: Optional[str]
    other: Optional[str]


class OpenDate(BaseModel):
    """
    {
        "opens": str as iso8601 date,
        "closes": str as iso8601 date,
    }
    """

    opens: Optional[str]
    closes: Optional[str]


class OpenHour(BaseModel):
    """
    {
        "day": str as day of week enum e.g. monday,
        "opens": str as hh:mm,
        "closes": str as hh:mm,
    }
    """

    day: str
    open: str
    closes: str


class Availability(BaseModel):
    """
    {
        "drop_in": bool,
        "appointments": bool,
    },
    """

    drop_in: Optional[bool]
    appointments: Optional[bool]


class Vaccine(BaseModel):
    """
    {
        "vaccine": str as vaccine type enum,
        "supply_level": str as supply level enum e.g. more_than_48hrs
    }
    """

    vaccine: str
    supply_level: Optional[str]


class Access(BaseModel):
    """
    {
        "walk": bool,
        "drive": bool,
        "wheelchair": str,
    }
    """

    walk: Optional[bool]
    drive: Optional[bool]
    wheelchair: Optional[str]


class Organization(BaseModel):
    """
    {
        "id": str as parent organization enum e.g. rite_aid,
        "name": str,
    }
    """

    id: Optional[str]
    name: Optional[str]


class Link(BaseModel):
    """
    {
        "authority": str as authority enum e.g. rite_aid or google_places,
        "id": str as id used by authority to reference this location e.g. 4096,
        "uri": str as uri used by authority to reference this location,
    }
    """

    authority: Optional[str]
    id: Optional[str]
    uri: Optional[str]


class Source(BaseModel):
    """
    {
        "source": str as source type enum e.g. vaccinespotter,
        "id": str as source defined id e.g. 7382088,
        "fetched_from_uri": str as uri where data was fetched from,
        "fetched_at": str as iso8601 datetime (when scraper ran),
        "published_at": str as iso8601 datetime (when source claims it updated),
        "data": {...parsed source data in source schema...},
    }
    """

    source: str
    id: str
    fetched_from_uri: Optional[str]
    fetched_at: Optional[str]
    published_at: Optional[str]
    data: dict


class NormalizedLocation(BaseModel):
    id: str
    name: Optional[str]
    address: Optional[Address]
    location: Optional[LatLng]
    contact: Optional[List[Contact]]
    languages: Optional[List[str]]  # [str as ISO 639-1 code]
    opening_dates: Optional[List[OpenDate]]
    opening_hours: Optional[List[OpenHour]]
    availability: Optional[Availability]
    inventory: Optional[List[Vaccine]]
    access: Optional[Access]
    parent_organization: Optional[Organization]
    links: Optional[List[Link]]
    notes: Optional[List[str]]
    active: Optional[bool]
    source: Source


class ImportMatchAction(BaseModel):
    """Match action to take when importing a source location"""

    id: Optional[str]
    action: str


class ImportSourceLocation(BaseModel):
    """Import source location record"""

    source_uid: str
    source_name: str
    name: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    import_json: dict
    match: Optional[ImportMatchAction]
