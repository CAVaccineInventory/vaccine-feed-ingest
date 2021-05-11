from vaccine_feed_ingest_schema.location import VaccineProvider

from vaccine_feed_ingest.utils.normalize import provider_id_from_name


def test_provider_id_from_name():
    # positive cases
    _test_provider("Acme Pharmacy #1058", VaccineProvider.ACME, "1058")

    _test_provider("Albertsons Pharmacy #1915", VaccineProvider.ALBERTSONS, "1915")
    _test_provider(
        "Albertsons Market Pharmacy #1915", VaccineProvider.ALBERTSONS, "1915"
    )

    _test_provider("Big Y Pharmacy #17 Rx #408532", VaccineProvider.BIG_Y, "408532")
    _test_provider("Big Y Pharmacy #067209", VaccineProvider.BIG_Y, "67209")

    _test_provider(
        "Brookshire Pharmacy #14 #351329", VaccineProvider.BROOKSHIRE, "351329"
    )

    _test_provider("COSTCO MARKET PHARMACY #122", VaccineProvider.COSTCO, "122")
    _test_provider("COSTCO PHARMACY #122", VaccineProvider.COSTCO, "122")
    _test_provider("Costco Wholesale Corporation #1062", VaccineProvider.COSTCO, "1062")
    _test_provider("Costco Pharmacy # 1014", VaccineProvider.COSTCO, "1014")

    _test_provider("Cub Pharmacy #122 #242356", VaccineProvider.CUB, "242356")

    _test_provider("Cvs 104", VaccineProvider.CVS, "104")
    _test_provider("Cvs Store 104", VaccineProvider.CVS, "104")
    _test_provider("Cvs Pharmacy 104", VaccineProvider.CVS, "104")
    _test_provider("Cvs StorePharmacy 104", VaccineProvider.CVS, "104")
    _test_provider("Cvs StorePharmacy, Inc. 104", VaccineProvider.CVS, "104")
    _test_provider("Cvs Store #104", VaccineProvider.CVS, "104")
    _test_provider("Cvs Pharmacy #104", VaccineProvider.CVS, "104")
    _test_provider("Cvs StorePharmacy #104", VaccineProvider.CVS, "104")
    _test_provider("Cvs StorePharmacy, Inc #104", VaccineProvider.CVS, "104")
    _test_provider("Cvs StorePharmacy, Inc. #104", VaccineProvider.CVS, "104")
    _test_provider("CVS Pharmacy, Inc. #008104", VaccineProvider.CVS, "8104")

    _test_provider("Dillon's Pharmacy #61500005", VaccineProvider.DILLONS, "61500005")

    _test_provider("Drugco Discount Pharmacy #3425", VaccineProvider.DRUGCO, "3425")

    _test_provider(
        "Family  Fare   Pharmacy 0115 #5221", VaccineProvider.FAMILY_FARE, "5221"
    )
    _test_provider(
        "Family Fare Pharmacy #0115 #5221", VaccineProvider.FAMILY_FARE, "5221"
    )

    _test_provider("Food City Pharmacy #0115 #5221", VaccineProvider.FOOD_CITY, "5221")

    _test_provider("Food Lion #5221", VaccineProvider.FOOD_LION, "5221")

    _test_provider("Fred Meyer #5221", VaccineProvider.FRED_MEYER, "5221")
    _test_provider("Fred Meyer Pharmacy #5221", VaccineProvider.FRED_MEYER, "5221")

    _test_provider("Fry's Food And Drug #66000061", VaccineProvider.FRYS, "66000061")

    _test_provider(
        "Genoa Healthcare 00010 (Chattanooga - Bell Avenue)",
        VaccineProvider.GENOA,
        "10",
    )
    _test_provider("Genoa Healthcare LLC #00072", VaccineProvider.GENOA, "72")

    _test_provider(
        "Giant Eagle Pharmacy #0012 #G00122", VaccineProvider.GIANT_EAGLE, "122"
    )

    _test_provider("Giant Food #198", VaccineProvider.GIANT_FOOD, "198")

    _test_provider("Giant #6269", VaccineProvider.GIANT, "6269")

    _test_provider("Haggen Pharmacy #3450", VaccineProvider.HAGGEN, "3450")

    _test_provider("Hannaford #3450", VaccineProvider.HANNAFORD, "3450")

    _test_provider("Harmons Pharmacy #18", VaccineProvider.HARMONS, "18")

    _test_provider("Harps Pharmacy #177", VaccineProvider.HARPS, "177")

    _test_provider(
        "Harris Teeter Pharmacy #09700030", VaccineProvider.HARRIS_TEETER, "9700030"
    )

    _test_provider("Hartig Drug Co #34 #324429", VaccineProvider.HARTIG, "324429")
    _test_provider("Hartig Drug Co 26 #324429", VaccineProvider.HARTIG, "324429")

    _test_provider("H-E-B #198", VaccineProvider.HEB, "198")

    _test_provider("Homeland Pharmacy #24429", VaccineProvider.HOMELAND, "24429")

    _test_provider("Hy-Vee Inc. #449", VaccineProvider.HY_VEE, "449")

    _test_provider("Ingles Pharmacy #031 #438575", VaccineProvider.INGLES, "438575")

    _test_provider(
        "KAISER HEALTH PLAN B PHY 722", VaccineProvider.KAISER_HEALTH_PLAN, "722"
    )
    _test_provider(
        "KAISER HEALTH PLAN BLDG PHY 632", VaccineProvider.KAISER_HEALTH_PLAN, "632"
    )
    _test_provider(
        "KAISER HEALTH PLAN MOB 1 PHY 511", VaccineProvider.KAISER_HEALTH_PLAN, "511"
    )

    _test_provider(
        "KAISER PERMANENTE PHARMACY #054", VaccineProvider.KAISER_PERMANENTE, "54"
    )

    _test_provider(
        "King Soopers Pharmacy #62000121", VaccineProvider.KING_SOOPERS, "62000121"
    )
    _test_provider(
        "King Soopers Pharmacy 62000001", VaccineProvider.KING_SOOPERS, "62000001"
    )

    _test_provider("Kroger Pharmacy #01100330", VaccineProvider.KROGER, "1100330")
    _test_provider("Kroger Pharmacy 743", VaccineProvider.KROGER, "743")

    _test_provider("Mariano's Pharmacy #53100520", VaccineProvider.MARIANOS, "53100520")

    _test_provider("Medicap Pharmacy #8207 #162543", VaccineProvider.MEDICAP, "162543")

    _test_provider("Meijer #312", VaccineProvider.MEIJER, "312")

    _test_provider("The Little Clinic #36213", VaccineProvider.LITTLE_CLINIC, "36213")

    _test_provider("Market Street Pharmacy #531", VaccineProvider.MARKET_STREET, "531")

    _test_provider("Osco Drug #3272", VaccineProvider.OSCO, "3272")
    _test_provider("Osco Pharmacy #3272", VaccineProvider.OSCO, "3272")

    _test_provider("Pavilions PHARMACY #8545", VaccineProvider.PAVILIONS, "8545")

    _test_provider(
        "Pick N Save Pharmacy #53400102", VaccineProvider.PICK_N_SAVE, "53400102"
    )

    _test_provider(
        "Price Chopper Pharmacy 184 #039929", VaccineProvider.PRICE_CHOPPER, "39929"
    )
    _test_provider(
        "Price Chopper Pharmacy #20 #MS1004804",
        VaccineProvider.PRICE_CHOPPER,
        "1004804",
    )

    _test_provider("Publix Super Markets Inc. #850", VaccineProvider.PUBLIX, "850")

    _test_provider("QFC Pharmacy #70500126", VaccineProvider.QFC, "70500126")

    _test_provider("RALEY'S PHARMACY #339", VaccineProvider.RALEYS, "339")

    _test_provider("Rite Aid Pharmacy 3897", VaccineProvider.RITE_AID, "3897")
    _test_provider("Rite Aid #RA100216", VaccineProvider.RITE_AID, "100216")

    _test_provider("Safeway #85", VaccineProvider.SAFEWAY, "85")
    _test_provider("Safeway PHARMACY #85", VaccineProvider.SAFEWAY, "85")

    _test_provider("Sam's Pharmacy #6549", VaccineProvider.SAMS, "6549")
    _test_provider("Sams Club #10-6371", VaccineProvider.SAMS, "6371")
    _test_provider("Sams Club 6331", VaccineProvider.SAMS, "6331")
    _test_provider("Sams Pharmacy 10-6371", VaccineProvider.SAMS, "6371")

    _test_provider("SAV-ON PHARMACY #585", VaccineProvider.SAV_ON, "585")
    _test_provider("SAVON PHARMACY #585", VaccineProvider.SAV_ON, "585")

    _test_provider("Shoprite Pharmacy #447", VaccineProvider.SHOP_RITE, "447")

    _test_provider("Smith's Pharmacy #70600075", VaccineProvider.SMITHS, "70600075")

    _test_provider("Stop & Shop #694", VaccineProvider.STOP_AND_SHOP, "694")

    _test_provider("Tom Thumb Pharmacy #2554", VaccineProvider.TOM_THUMB, "2554")

    _test_provider("Thrifty Drug Stores Inc #5", VaccineProvider.THRIFTY, "5")

    _test_provider("Vons Pharmacy #675", VaccineProvider.VONS, "675")

    _test_provider("Walgreens #24295", VaccineProvider.WALGREENS, "24295")
    _test_provider("Walgreens Co. #1158", VaccineProvider.WALGREENS, "1158")
    _test_provider("Walgreens Pharmacy #24295", VaccineProvider.WALGREENS, "24295")
    _test_provider(
        "Walgreens Specialty Pharmacy #35326", VaccineProvider.WALGREENS, "35326"
    )
    _test_provider("Walgreens Specialty #24295", VaccineProvider.WALGREENS, "24295")

    _test_provider("Walmart Inc, #2509", VaccineProvider.WALMART, "2509")
    _test_provider("Walmart Inc #10-1165", VaccineProvider.WALMART, "1165")
    _test_provider("Walmart PHARMACY 10-585", VaccineProvider.WALMART, "585")
    _test_provider("Walmart Pharmacy #1001", VaccineProvider.WALMART, "1001")

    _test_provider("Weis Pharmacy #227 #801371", VaccineProvider.WEIS, "801371")

    _test_provider("Winn-Dixie #435", VaccineProvider.WINN_DIXIE, "435")

    # negative cases
    assert provider_id_from_name("garbage") is None
    assert provider_id_from_name("Walblue 232555") is None


def _test_provider(input_str, expected_provider_name, expected_provider_id):
    result = provider_id_from_name(input_str)

    assert result
    actual_provider_name, actual_provider_id = result

    assert actual_provider_name == expected_provider_name
    assert actual_provider_id == expected_provider_id
