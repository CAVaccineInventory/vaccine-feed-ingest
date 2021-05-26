from vaccine_feed_ingest_schema import location as schema

from vaccine_feed_ingest.utils.normalize import normalize_address, parse_address


def test_parse_address():
    assert parse_address("abcdefgh").get("AddressNumber") is None

    assert (
        parse_address("1600 Pennsylvania Ave NW, Washington DC, 20500").get("ZipCode")
        == "20500"
    )


def test_normalize_address():
    assert normalize_address(parse_address("qwertyuiop")) == schema.Address(
        street1="qwertyuiop",
        street2=None,
        city=None,
        state=None,
        zip=None,
    )

    assert normalize_address(
        parse_address("1600 Pennsylvania Ave NW, Washington DC, 20500")
    ) == schema.Address(
        street1="1600 Pennsylvania Ave NW",
        city="Washington",
        state="DC",
        zip="20500",
    )

    # Superfluous ", USA" removal.
    assert normalize_address(
        parse_address("1060 W Addison St, Chicago, IL, USA 60613")
    ) == schema.Address(
        street1="1060 W Addison St",
        city="Chicago",
        state="IL",
        zip="60613",
    )

    # Newline handling.
    assert normalize_address(
        parse_address("Yosemite Falls\nYosemite Village, CA\n95389\n")
    ) == schema.Address(
        street1="Yosemite Falls",
        street2=None,  # This is a weird artifact.
        city="Yosemite Village",
        state="CA",
        zip="95389",
    )

    assert normalize_address(
        parse_address("3720 S Las Vegas Blvd\nSpace 265\nLas Vegas, NV 89158")
    ) == schema.Address(
        street1="3720 S Las Vegas Blvd",
        street2="Space 265",
        city="Las Vegas",
        state="NV",
        zip="89158",
    )
