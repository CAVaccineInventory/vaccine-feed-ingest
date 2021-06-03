from vaccine_feed_ingest_schema import location

from vaccine_feed_ingest.stages import enrichment


def test_add_provider_from_name_minimal(minimal_location):
    enrichment._add_provider_from_name(minimal_location)


def test_add_provider_from_name(full_location):
    # Clear parent prganization to check
    full_location.parent_organization = None
    assert not full_location.parent_organization

    links = enrichment._generate_link_map(full_location)
    assert "rite_aid" not in links

    enrichment._add_provider_from_name(full_location)

    links = enrichment._generate_link_map(full_location)
    assert "rite_aid" in links
    assert links["rite_aid"] == "5952"

    assert full_location.parent_organization
    assert str(full_location.parent_organization.id) == "rite_aid"


def test_add_source_link_minimal(minimal_location):
    enrichment._add_source_link(minimal_location)


def test_add_source_link(full_location):
    # Clear parent prganization to check
    full_location.links = None

    enrichment._add_source_link(full_location)

    links = enrichment._generate_link_map(full_location)
    assert full_location.source.source in links
    assert links[full_location.source.source] == full_location.source.id


def test_normalize_phone_format(minimal_location):
    minimal_location.contact = [
        location.Contact(phone="(800) 456-7890"),
        location.Contact(phone="1-415-789-3456"),
        location.Contact(phone="+1 (415)888-8888"),
        location.Contact(phone="888-888-8888 x8888888888"),
    ]

    enrichment._normalize_phone_format(minimal_location)

    assert len(minimal_location.contact) == 4

    expected = {
        "(800) 456-7890",
        "(415) 789-3456",
        "(415) 888-8888",
        "888-888-8888 x8888888888",
    }
    actual = {entry.phone for entry in minimal_location.contact if entry.phone}

    assert expected == actual


def test_add_provider_tag_minimal(minimal_location):
    enrichment._add_provider_tag(minimal_location)


def test_add_provider_tag(full_location):
    enrichment._add_provider_tag(full_location)

    links = enrichment._generate_link_map(full_location)
    assert enrichment.PROVIDER_TAG in links
    assert links[enrichment.PROVIDER_TAG] == str(full_location.parent_organization.id)


def test_process_location_minimal(minimal_location):
    enrichment._process_location(minimal_location)
    assert minimal_location


def test_process_location(full_location):
    enrichment._process_location(full_location)
    assert full_location


def test_bulk_process_locations(full_location, minimal_location):
    locations = [full_location, minimal_location]
    enrichment._bulk_process_locations(locations)
    assert locations


def test_is_loadable_location(full_location, minimal_location):
    assert enrichment._is_loadable_location(full_location) is True
    assert enrichment._is_loadable_location(minimal_location) is False
