from vaccine_feed_ingest.stages import enrichment


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


def test_add_source_link(full_location):
    # Clear parent prganization to check
    full_location.links = None

    enrichment._add_source_link(full_location)

    links = enrichment._generate_link_map(full_location)
    assert full_location.source.source in links
    assert links[full_location.source.source] == full_location.source.id
