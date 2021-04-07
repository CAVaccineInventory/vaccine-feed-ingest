#!/usr/bin/env python3

from typing import NamedTuple

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
    provider_name: str = None


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
        "provider": {
            "id": loc.provider_id,
            "name": loc.provider_name,
        },
    }
    return d
