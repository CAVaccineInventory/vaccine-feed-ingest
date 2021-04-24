'''
This script uses Selenium to collect data from the NC official website, by querying per NC zip code. The zip codes
are collected through the requests library via a third party website. The script takes about 4 minutes to run on
my personal computer and produces an html file
for each zip code.
'''

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
import os
import re
import requests
import sys


def connect_to_website(website_link):
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument("--test-type")
    driver = webdriver.Chrome(options=options)
    driver.get(website_link)

    timeout_in_secs = 20
    try:
        spinner_xpath = '/html/body/app-root/ngx-spinner/div/div[1]'
        WebDriverWait(driver, timeout_in_secs).until(
            expected_conditions.invisibility_of_element_located((By.XPATH, spinner_xpath)))
    except NoSuchElementException:
        # Spinner element is no longer there, leading to an exception being thrown.
        pass
    # The website loads with a spinner and doesn't allow vaccine location querying while the spinner is loading

    return driver


def get_data_html_from_website_driver(driver, search_term):
    try:
        text_input_id = "mat-input-0"
        text_input = driver.find_element_by_id(text_input_id)
        text_input.send_keys(search_term)
        text_input.send_keys(Keys.ENTER)

        data_xpath = "/html/body/app-root/app-map-view/div/div/div[3]/div[1]"
        data_element = driver.find_element_by_xpath(data_xpath)

        text_input.clear()
        return data_element.get_attribute('innerHTML')
    except Exception as e:
        text_input.clear()
        return "Failed to collect data for this search term. This might be due to no results. Exception: " + str(e)


# Prints raw html to a file. Should be relatively easy to parse.
# Note - replaced 'clear' and 'done` with '*NotAvailable*' and '*Available*' to make it clearer for whoever does
# the parsing.
# If neither '*NotAvailable*' nor '*Available*' appear for a location, the location doesn't have available doses.
def write_data_html_to_file(filename, data_html):
    with open(filename, "w") as f_out:
        data_to_write = data_html.replace("clear", "*NotAvailable*")
        data_to_write = data_to_write.replace("done", "*Available*")
        f_out.write(data_to_write)


# Default website includes conveniently styled information on North Carolina zip codes.
def get_nc_zip_codes(zip_code_website="https://www.zip-codes.com/state/nc.asp"):
    nc_zip_codes_html_data = requests.get(zip_code_website)
    zip_code_regex = "ZIP[ ]Code[ ]\\d{5}"
    zip_codes_as_strings = re.findall(zip_code_regex, nc_zip_codes_html_data.text)
    unique_zip_codes_as_strings = list(set([x.replace("ZIP Code ", "") for x in zip_codes_as_strings]))
    unique_zip_codes_as_strings.sort()
    return unique_zip_codes_as_strings


output_directory = sys.argv[1]

# Official North Carolina website. Requires querying per location.
nc_driver = connect_to_website('https://myspot.nc.gov')

# Runs through all the zip codes in North Carolina. Takes a few minutes to run.
# Note 1 - Duplicate locations might appear between zip codes that are near each other.
# Note 2 - This took about 4 minutes to run on my personal computer.
nc_zip_codes = get_nc_zip_codes()
for nc_zip_code in nc_zip_codes:
    data_html = get_data_html_from_website_driver(nc_driver, nc_zip_code)
    output_filename = os.path.join(output_directory, nc_zip_code + ".html")
    write_data_html_to_file(output_filename, data_html)
