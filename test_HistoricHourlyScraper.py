from HistoricHourlyScraper import HistoricHourlyScraper
import pytest

"""
Unit testing of HistoricHourlyScraper methods
Note, several methods from HistoricHourlyScraper.py cannot be tested using pytest
since they return information regarding the current date and weather observations
therein.  To test get_today(), get_today_weather(), and get_past_hour(), one
must do visual testing where they run these methods, and then go to the
IEM website to manually check that these methods are behaving properly.
Devon checked these methods on April 15th 2021 both early in the day (8AM) and
in the evening (10PM) and these functions appeared to be working properly.  
"""

h = HistoricHourlyScraper()

def test_attr():
    attribute = h.days
    assert attribute == {'01': '31', '02': '28', '03': '31', '04': '30', '05': '31', '06': '30', '07': '31', '08': '31', '09': '30', '10': '31', '11': '30', '12': '31'}

def test_time():
    time1 = '17:00'
    time2 = '3:00'
    incorrect_time = "Three o'clock"
    incorrect_type = 1
    assert h.convert_time(time1) == '6:00PM'
    assert h.convert_time(time2) == '4:00AM'
    assert h.convert_time(incorrect_time) == None
    assert h.convert_time(incorrect_type) == None

def test_convert_date():
    date1 = '2020-01-01 17:00'
    date2 = '2020-01-01 23:00'
    date3 = '2020-01-01 11:00'
    incorrect_date = '01-2020-01 17:00'
    no_time = '2020-01-01'
    incorrect_type = [0]
    assert h.convert_date(date1) == ('Jan012020', '6:00PM')
    assert h.convert_date(date2) == ('Jan012020', '12:00AM')
    assert h.convert_date(date3) == ('Jan012020', '12:00PM')
    assert h.convert_date(incorrect_date) == None
    assert h.convert_date(no_time) == None
    assert h.convert_date(incorrect_type) == None

def test_construct_dict():
    # Correct dictionary info
    c_temp = '69'
    c_when = '2020-01-01 16:00'      # make sure this is in the form of the date on the IEM webiste
    c_precip = '45'

    # Incorrect dictionary info (trying to insert gobbledegook into construct_dict)
    i_temp = 'yahoo'
    i_when = (12, '5 oclock')
    i_precip = ['candy apples']

    assert h.construct_dict(c_temp, c_when, c_precip) == {'Date':'Jan012020', 'Time':'5:00PM', 'Temperature':'69', 'Precipitation': '45'}
    assert h.construct_dict(i_temp, i_when, i_precip) == None

def test_visit_url():
    good_url = 'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1=2020&month1=12&day1=31&year2=2020&month2=12&day2=31&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2'
    bad_url = 'bad url'
    wrong_type = 7
    
    assert type(h.visit_url(good_url)) == list
    assert h.visit_url(bad_url) == None
    assert h.visit_url(wrong_type) == None
    
def test_extract_data():
    # Following URL is hourly weather data for January 1st 2020
    iem_snippet_url = 'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1=2020&month1=1&day1=1&year2=2020&month2=1&day2=1&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2'
    result = h.extract_data(iem_snippet_url)
    result1_to_check = result[10]   # Pick an extracted data point to check
    result2_to_check = result[22]   # Pick a second extracted data point to check
    assert result1_to_check == {'Date':'Jan012020', 'Time':'11:00AM', 'Temperature':'32.0','Precipitation':'No precipitation'}
    assert result2_to_check == {'Date':'Jan012020', 'Time':'11:00PM', 'Temperature':'30.0','Precipitation':'No precipitation'}                    

def test_fetch_many_observations():
    obs_list = h.fetch_many_observations(test=True)
    check1 = obs_list[5]
    check2 = obs_list[23]
    assert check1 == {'Date':'Jan012019', 'Time':'6:00AM', 'Temperature':'48.0', 'Precipitation':'No precipitation'}
    assert check2 == {'Date':'Jan012019', 'Time':'12:00AM', 'Temperature':'35.1', 'Precipitation':'Trace amount'}
    

