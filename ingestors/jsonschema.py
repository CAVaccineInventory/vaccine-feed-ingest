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


def to_dict(l):
    """ Convert Location namedtuple to nested dict """

    d = {
        "id": l.id,
        "name": l.name,
        "address": {
            "street1": l.street1,
            "street2": l.street2,
            "city": l.city,
            "state": l.state,
            "zip": l.zip,
        },
        "location": {
            "latitude": l.latitude,
            "longitude": l.longitude,
        },
        "contact": {
            "phone": [l.phone],
            "website": [l.website],
            "email": [l.email],
            "other": [l.contact_other],
        },
        "provider": {
            "id": l.provider_id,
            "name": l.provider_name,
        },
    }
    return d
