import HistoricScraper
from HistoricScraper import HistoricScraper
import pytest

"""
Unit Test to determine if weather data collected from the scraper for a specific
date matches the actual expected weather date for the specified date.
"""
def test_scraper():
    h = HistoricScraper()
    data = h.scraper()
    dict1 = data[0][0].items()
    dict1_test = {'Month': 'APRIL', 'Year': '2021', 'Day': '1', 'HighTemp': '39', 'LowTemp': '24', 'Rainfall': '0.09', 'Snowfall': '0.8'}
    dict1_test = dict1_test.items()

    assert dict1 == dict1_test
