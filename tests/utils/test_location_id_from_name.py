from vaccine_feed_ingest.utils.parse import location_id_from_name


def _test(input_name, expected_output_id):
    assert location_id_from_name(input_name) == expected_output_id


def test_location_id_from_name():
    _test("", "")
    _test("(*%,^", "")
    _test("   ---   ", "_")
    _test("Foo Bar", "foo_bar")
    _test("Foo-Bar", "foo_bar")
    _test("Foo - #Bar", "foo_bar")
    _test("Acadia St. Landry Hospital", "acadia_st_landry_hospital")
    _test("Walgreens #09862", "walgreens_09862")
    _test("Walgreens # 09862", "walgreens_09862")
    _test("Walgreens   #09862", "walgreens_09862")
    _test(" Walgreens   #09862 ", "walgreens_09862")
    _test("Walgreens - #09862", "walgreens_09862")
    _test("Walmart Pharmacy #310 - Crowley", "walmart_pharmacy_310_crowley")
    _test(
        "Carmichael's Cashway Pharmacy - Crowley",
        "carmichaels_cashway_pharmacy_crowley",
    )
    _test("Winn-Dixie #1590", "winn_dixie_1590")
    _test("*** Cottonport Corner Drug", "_cottonport_corner_drug")
    _test(
        "Avoyelles Parish Health Unit (at Paragon Casino)",
        "avoyelles_parish_health_unit_at_paragon_casino",
    )
    _test("Albertsons/Savon #0218", "albertsonssavon_0218")
    _test("K & S Drugs", "k_s_drugs")
    _test("Stewart\u2019s Drug Store", "stewarts_drug_store")
    _test("OrthoLA", "orthola")
