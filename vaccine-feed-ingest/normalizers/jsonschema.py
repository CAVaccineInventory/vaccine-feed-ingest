#!/usr/bin/env python3

from typing import NamedTuple

"""
This module provides a flat Location structure so scraper authors do not need
to understand the normalized ndjson spec:

https://docs.google.com/document/d/1qxABDlRep76llrXhgwdwA6Ji3Jub-MftRvustk12XHc
"""


class Location(NamedTuple):
    id: str = None
    name: str = None
    street1: str = None
    street2: str = None
    city: str = None
    state: str = None
    zip: str = None
    latitude: str = None
    longitude: str = None
    phone: str = None
    website: str = None
    email: str = None
    contact_other: str = None
    provider_id: str = None
    provider_brand: str = None
    provider_brand_name: str = None
    booking_website: str = None
    booking_phone: str = None
    booking_email: str = None
    appointments_available: bool = False
    fetched_at: str = None
    published_at: str = None
    fetched_from_uri: str = None
    source: str = None
    data: dict[str, str] = None


def omit_empty(d):
    def pop_empty_values(d):
        for key, value in list(d.items()):
            if isinstance(value, dict):
                omit_empty(value)
            elif value is None:
                d.pop(key)
            elif value == [None]:
                d.pop(key)

    pop_empty_values(d)
    for key, value in list(d.items()):
        if value == {}:
            d.pop(key)
    return d


def to_dict(loc):
    """ Convert Location namedtuple to nested dict """

    d = {
        "id": loc.id,
        "name": loc.name,
        "address": {
            "street1": loc.street1,
            "street2": loc.street2,
            "city": loc.city,
            "state": loc.state,
            "zip": loc.zip,
        },
        "location": {
            "latitude": loc.latitude,
            "longitude": loc.longitude,
        },
        "contact": {
            "phone": [loc.phone],
            "website": [loc.website],
            "email": [loc.email],
            "other": [loc.contact_other],
        },
        "booking": {
            "phone": loc.booking_phone,
            "website": loc.booking_website,
            "email": loc.booking_email,
        },
        "availability": {
            "appointments": loc.appointments_available,
        },
        "parent_organization": {
            "id": loc.provider_brand,
            "name": loc.provider_brand_name,
        },
        "links": [
            {
                "authority": loc.provider_brand,
                "id": loc.provider_id,
            }
        ],
        "fetched_at": loc.fetched_at,
        "published_at": loc.published_at,
        "sources": [
            {
                "source": loc.source,
                "id": loc.id,
                "fetched_from_uri": loc.fetched_from_uri,
                "fetched_at": loc.fetched_at,
                "published_at": loc.published_at,
                "data": loc.data,
            }
        ],
    }

    d = omit_empty(d)
    return d
