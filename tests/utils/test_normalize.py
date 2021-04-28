from vaccine_feed_ingest.utils import normalize

def test_provider_from_name():
    assert normalize.organization_from_name("Walgreens") == ("walgreens", "Walgreens")
    assert normalize.organization_from_name("walgrEEns") == ("walgreens", "Walgreens")
    assert normalize.organization_from_name("Walgreens Specialty Pharmacy #123") == ("walgreens", "Walgreens")

    assert normalize.organization_from_name("CVS") == ("cvs", "CVS")
    assert normalize.organization_from_name("cVs") == ("cvs", "CVS")
    assert normalize.organization_from_name("CVS Store Pharmacy #123") == ("cvs", "CVS")

    assert normalize.organization_from_name("Costco") == ("costco", "Costco")
    assert normalize.organization_from_name("costCO") == ("costco", "Costco")

