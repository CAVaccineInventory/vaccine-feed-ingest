from vaccine_feed_ingest.utils import match


def test_is_address_similar(full_location, vial_location):
    assert match.is_address_similar(full_location, vial_location)


def test_is_provider_similar(full_location, vial_location):
    assert match.is_provider_similar(full_location, vial_location)


def test_has_matching_phone_number(full_location, vial_location):
    assert match.has_matching_phone_number(full_location, vial_location)
