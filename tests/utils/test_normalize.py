from vaccine_feed_ingest.utils import normalize


def test_calculate_content_hash(full_location):
    original_hash = normalize.calculate_content_hash(full_location)
    assert original_hash

    original_hash_again = normalize.calculate_content_hash(full_location)
    assert original_hash_again

    assert original_hash_again == original_hash

    full_location.source.fetched_at = "1980-01-01T01:01:01"

    source_modified_hash = normalize.calculate_content_hash(full_location)
    assert source_modified_hash

    assert source_modified_hash == original_hash

    full_location.name = "New Location Name"

    name_modified_hash = normalize.calculate_content_hash(full_location)
    assert name_modified_hash

    assert name_modified_hash != original_hash
