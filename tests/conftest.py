import pytest
from vaccine_feed_ingest_schema import location


@pytest.fixture
def minimal_location():
    return location.NormalizedLocation(
        id="source:id",
        source=location.Source(
            source="source",
            id="id",
            data={"id": "id"},
        ),
    )


@pytest.fixture
def full_location():
    return location.NormalizedLocation(
        id="source:dad13365-2202-4dea-9b37-535288b524fe",
        name="Rite Aid Pharmacy 5952",
        address=location.Address(
            street1="1991 Mountain Boulevard",
            city="Oakland",
            state="CA",
            zip="94611",
        ),
        location=location.LatLng(
            latitude=37.8273167,
            longitude=-122.2105179,
        ),
        contact=[
            location.Contact(
                contact_type=location.ContactType.BOOKING,
                phone="(916) 445-2841",
            ),
            location.Contact(
                contact_type=location.ContactType.GENERAL,
                phone="(510) 339-2215",
            ),
        ],
        languages=["en"],
        opening_dates=[
            location.OpenDate(
                opens="2021-04-01",
                closes="2021-04-01",
            ),
        ],
        opening_hours=[
            location.OpenHour(
                day=location.DayOfWeek.MONDAY,
                opens="08:00",
                closes="14:00",
            ),
        ],
        availability=location.Availability(
            drop_in=False,
            appointments=True,
        ),
        inventory=[
            location.Vaccine(
                vaccine=location.VaccineType.MODERNA,
                supply_level=location.VaccineSupply.IN_STOCK,
            ),
        ],
        access=location.Access(
            walk=True,
            drive=False,
            wheelchair=location.WheelchairAccessLevel.PARTIAL,
        ),
        parent_organization=location.Organization(
            id=location.VaccineProvider.RITE_AID,
            name="Rite Aid Pharmacy",
        ),
        links=[
            location.Link(
                authority=location.LocationAuthority.GOOGLE_PLACES,
                id="abc123",
            ),
        ],
        notes=["long note goes here"],
        active=True,
        source=location.Source(
            source="source",
            id="dad13365-2202-4dea-9b37-535288b524fe",
            fetched_from_uri="https://example.org",
            fetched_at="2020-04-04T04:04:04.4444",
            published_at="2020-04-04T04:04:04.4444",
            data={"id": "dad13365-2202-4dea-9b37-535288b524fe"},
        ),
    )


@pytest.fixture
def vial_location():
    return {
        "type": "Feature",
        "properties": {
            "id": "recmq8dNXlV1yWipP",
            "name": "RITE AID PHARMACY 05952",
            "state": "CA",
            "latitude": 37.82733,
            "longitude": -122.21058,
            "location_type": "Pharmacy",
            "import_ref": "vca-airtable:recmq8dNXlV1yWipP",
            "phone_number": "510-339-2215",
            "full_address": "1991 MOUNTAIN BOULEVARD, OAKLAND, CA 94611",
            "county": "Alameda",
            "google_places_id": "ChIJA0MOOYWHj4ARW8M-vrC9yGA",
            "vaccinefinder_location_id": "ed4af07f-1a17-408b-b705-7c982b7d25d6",
            "vaccinespotter_location_id": "7384085",
            "hours": "Monday - Friday: 9:00 AM \u2013 9:00 PM\nSaturday: 9:00 AM \u2013 6:00 PM\nSunday: 10:00 AM \u2013 6:00 PM",
            "provider": {"name": "Rite-Aid Pharmacy", "type": "Pharmacy"},
            "concordances": [
                "google_places:ChIJA0MOOYWHj4ARW8M-vrC9yGA",
                "vaccinefinder:ed4af07f-1a17-408b-b705-7c982b7d25d6",
                "vaccinespotter_org:7384085",
            ],
        },
        "geometry": {"type": "Point", "coordinates": [-122.21058, 37.82733]},
    }
