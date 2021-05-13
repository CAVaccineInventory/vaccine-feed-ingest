from vaccine_feed_ingest.utils.normalize import normalize_phone


def test_normalize_phone():
    assert normalize_phone("") == []
    assert normalize_phone("abc") == []
    assert normalize_phone("1234") == []
    assert normalize_phone("1234567890") == []  # Not a valid phone-like number.

    assert normalize_phone("211/311") == ["211", "311"]

    assert normalize_phone("212 555 1212") == ["(212) 555-1212"]
    assert normalize_phone("(212) 555 1212") == ["(212) 555-1212"]

    assert normalize_phone("212 555 1212 ext17") == ["(212) 555-1212 ext. 17"]
    assert normalize_phone("212 555 1212 x17") == ["(212) 555-1212 ext. 17"]
    assert normalize_phone("212 555 1212 OPTION 17") == ["(212) 555-1212 ext. 17"]
    assert normalize_phone("212 555 1212, option 17") == ["(212) 555-1212 ext. 17"]
    assert normalize_phone("212 555 1212 PRESS 17") == ["(212) 555-1212 ext. 17"]
    assert normalize_phone("212 555 1212, press 17 to schedule") == [
        "(212) 555-1212 ext. 17"
    ]

    assert normalize_phone("212 555 1212 / 212 555 1213") == [
        "(212) 555-1212",
        "(212) 555-1213",
    ]

    # Unfortunately, the library doesn't currently support matching
    # vanity numbers, e.g.
    #
    # assert normalize_phone("1-800-GOOG-411") ==  ["(800) 466-4411"]
