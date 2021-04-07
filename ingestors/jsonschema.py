#!/usr/bin/env python3

from collections import namedtuple

Location = namedtuple(
    "Location",
    [
        "id",
        "name",
        "street1",
        "street2",
        "city",
        "state",
        "zip",
        "latitude",
        "longitude",
        "phone",
        "website",
        "email",
        "contact_other",
        "provider_id",
        "provider_name",
    ],
    defaults=[None] * 13,
)


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
