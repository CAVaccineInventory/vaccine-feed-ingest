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
            street2="#1",
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
            )
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
            name="Rite Aid",
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
