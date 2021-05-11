from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.utils import match


def test_is_concordance_similar(full_location, minimal_location, vial_location):
    assert match.is_concordance_similar(full_location, vial_location)

    assert not match.is_concordance_similar(minimal_location, vial_location)


def test_is_address_similar(full_location, minimal_location, vial_location):
    assert match.is_address_similar(full_location, vial_location)

    assert not match.is_address_similar(minimal_location, vial_location)


def test_is_provider_similar(full_location, minimal_location, vial_location):
    assert match.is_provider_similar(full_location, vial_location)

    assert not match.is_provider_similar(minimal_location, vial_location)


def test_is_provider_tag_similar(full_location, minimal_location, vial_location):
    full_location.links.append(location.Link(authority="_tag_provider", id="rite_aid"))
    vial_location["properties"]["concordances"].append("_tag_provider:rite_aid")

    assert match.is_provider_tag_similar(full_location, vial_location)

    assert not match.is_provider_tag_similar(minimal_location, vial_location)


def test_is_phone_number_similar(full_location, minimal_location, vial_location):
    assert match.is_phone_number_similar(full_location, vial_location)

    assert not match.is_phone_number_similar(minimal_location, vial_location)
