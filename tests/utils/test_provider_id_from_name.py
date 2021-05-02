from vaccine_feed_ingest.utils.normalize import provider_id_from_name


def test_provider_id_from_name():
    # positive cases
    _test_provider("Rite Aid Pharmacy 3897", "rite_aid", "3897")
    _test_provider("Walgreens #24295", "walgreens", "24295")
    _test_provider("Walgreens Pharmacy #24295", "walgreens", "24295")
    _test_provider("Walgreens Specialty Pharmacy #35326", "walgreens", "35326")
    _test_provider("Walgreens Specialty #24295", "walgreens", "24295")
    _test_provider("Safeway #85", "safeway", "85")
    _test_provider("Safeway PHARMACY #85", "safeway", "85")
    _test_provider("Vons Pharmacy #675", "vons", "675")
    _test_provider("SAV-ON PHARMACY #585", "albertsons", "585")
    _test_provider("SAVON PHARMACY #585", "albertsons", "585")
    _test_provider("Pavilions PHARMACY #8545", "pavilions", "8545")
    _test_provider("Walmart PHARMACY 10-585", "walmart", "585")
    _test_provider("Cvs 104", "cvs", "104")
    _test_provider("Cvs Store 104", "cvs", "104")
    _test_provider("Cvs Pharmacy 104", "cvs", "104")
    _test_provider("Cvs StorePharmacy 104", "cvs", "104")
    _test_provider("Cvs StorePharmacy, Inc. 104", "cvs", "104")
    _test_provider("Cvs Store #104", "cvs", "104")
    _test_provider("Cvs Pharmacy #104", "cvs", "104")
    _test_provider("Cvs StorePharmacy #104", "cvs", "104")
    _test_provider("Cvs StorePharmacy, Inc #104", "cvs", "104")
    _test_provider("Cvs StorePharmacy, Inc. #104", "cvs", "104")

    # negative cases
    assert provider_id_from_name("garbage") is None
    assert provider_id_from_name("Walblue 232555") is None


def _test_provider(input_str, expected_provider_name, expected_provider_id):
    actual_provider_name, actual_provider_id = provider_id_from_name(input_str)
    assert actual_provider_name == expected_provider_name
    assert actual_provider_id == expected_provider_id
