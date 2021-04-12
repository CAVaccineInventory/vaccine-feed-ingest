#!/usr/bin/env python

"""
Vaccinate CA Schema
"""
import datetime
import enum
from typing import Optional, List

import pydantic


class BaseModel(pydantic.BaseModel):
    class Config:
        use_enum_values = True


class Address(BaseModel):
    street1: str
    street2: Optional[str]
    city: str
    state: str
    zip: str


class Location(BaseModel):
    latitude: float
    longitude: float


@enum.unique
class ContactType(str, enum.Enum):
    BOOKING = "booking"
    GENERAL = "general"


class Contact(BaseModel):
    contact_type: ContactType
    phone: Optional[str]
    website: Optional[str]
    email: Optional[str]
    other: Optional[str]


class OpeningDates(BaseModel):
    opens: Optional[datetime.date]
    closes: Optional[datetime.date]


@enum.unique
class DayOfWeek(str, enum.Enum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"
    PUBLIC_HOLIDAYS = "public_holidays"


class OpeningHours(BaseModel):
    day: DayOfWeek
    opens: Optional[datetime.time]
    closes: Optional[datetime.time]


class Availability(BaseModel):
    drop_in: Optional[bool]
    appointments: Optional[bool]


@enum.unique
class Vaccine(str, enum.Enum):
    PFIZER_BIONTECH = "pfizer_biontech"
    MODERNA = "moderna"
    JOHNSON_JOHNSON_JANSSEN = "johnson_johnson_janssen"
    OXFORD_ASTRAZENECA = "oxford_astrazeneca"


@enum.unique
class SuppyLevel(str, enum.Enum):
    LESS_THAN_48HRS = "less_than_48hrs"
    MORE_THAN_48HRS = "more_than_48hrs"
    OUT_OF_STOCK = "out_of_stock"


class Inventory(BaseModel):
    vaccine: Vaccine
    supply_level: Optional[SuppyLevel]
    in_stock: Optional[bool]


class Access(BaseModel):
    walk: Optional[bool]
    drive: Optional[bool]


class Organization(BaseModel):
    id: Optional[str]
    name: Optional[str]


@enum.unique
class Authority(str, enum.Enum):
    RITE_AID = "ride_aid"
    GOOGLE_PLACES = "google_places"


class Link(BaseModel):
    authority: Authority
    id: Optional[str]
    uri: Optional[str]


@enum.unique
class SourceFeed(str, enum.Enum):
    SF_GOV = "sf_gov"


class Source(BaseModel):
    source: SourceFeed
    id: str
    fetched_from_url: Optional[pydantic.AnyUrl]
    fetched_at: Optional[datetime.datetime]
    published_at: Optional[datetime.datetime]
    data: dict


class Facility(BaseModel):
    id: str
    name: Optional[str]

    address: Optional[Address]
    location: Optional[Location]

    contact: Optional[List[Contact]]

    opening_dates: Optional[List[OpeningDates]]
    opening_hours: Optional[List[OpeningHours]]

    availability: Optional[Availability]
    inventory: Optional[List[Inventory]]

    access: Optional[Access]
    languages: Optional[List[str]]

    parent_organization: Optional[List[Organization]]
    links: Optional[List[Link]]

    notes: Optional[List[str]]

    active: Optional[bool]

    fetched_at: Optional[datetime.datetime]
    published_at: Optional[datetime.datetime]

    sources: Optional[List[Source]]
