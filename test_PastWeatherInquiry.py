import pytest
import pymongo
import io
import sys
#from unittest.mock import mock
import PastWeatherInquiry
from PastWeatherInquiry import User_Interface_Historic


"""
Unit Test to see if the Single_Day_Inquiry function is able to successfully
query the database for weather data from a single, specified date.
"""
def test_Single_Day_Inquiry(capsys):
    client = pymongo.MongoClient("mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client["historic"]
    collection = db["daily"]
    date = ["Apr", "01", "2021", "-", "Apr", "02", "2021"]
    UI = User_Interface_Historic()
    s = UI.Single_Day_Inquiry(date, collection)
    print(s)
    captured = capsys.readouterr()
    expected = "For the date of  Apr 01 2021\nHigh Temperature:  39\nLow Temperature:  24\nRainfall:  0.09 in.\nSnowfall:  0.8 in.\nNone\n"
    assert captured.out == expected
    
"""
Unit Test to see if the Interval_Inquiry function is able to successfully
query the database for weather data from a specified interval of dates.
"""
def test_Interval_Inquiry(capsys):
    client = pymongo.MongoClient("mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client["historic"]
    collection = db["daily"]
    date = ["Apr", "01", "2021", "-", "Apr", "02", "2021"]
    UI = User_Interface_Historic()
    s = UI.Interval_Inquiry(date[0:3], date[4:], collection)
    print(s)
    captured = capsys.readouterr()
    
    expected_high = 'The High across these days is  40.0\n'
    expected_low = 'The Low across these days is  20.0\n'
    expected_rain = 'The Total snowfall across these days is  0.8 in.\n'
    expected_snow = 'The Total rainfall across these days is  0.09 in.\n'
                 
    assert expected_high in captured.out
    assert expected_low in captured.out
    assert expected_rain in captured.out
    assert expected_snow in captured.out

"""
Integrative Test to see if weather output in the UI is as expected for a single date
"""
def test_Historic_Inquiry_1_Date(capsys, monkeypatch):
    monkeypatch.setattr('sys.stdin', io.StringIO("Apr 01 2021\nexit\n"))
    UI = User_Interface_Historic()
    UI.Main()
    captured = capsys.readouterr()
    expected_high = 'High Temperature:  39\n'
    expected_low = 'Low Temperature:  24\n'
    expected_rain = 'Rainfall:  0.09 in.\n'
    expected_snow = 'Snowfall:  0.8 in.\n'
                 
    assert expected_high in captured.out
    assert expected_low in captured.out
    assert expected_rain in captured.out
    assert expected_snow in captured.out

"""
Integrative Test to see if weather output in the UI is as expected for a
given interval of dates
"""
def test_Historic_Inquiry_2_Interval(capsys, monkeypatch):
    monkeypatch.setattr('sys.stdin', io.StringIO("Apr 01 2021 - Apr 02 2021\nexit\n"))
    UI = User_Interface_Historic()
    UI.Main()
    captured = capsys.readouterr()
    expected_high = 'The High across these days is  40.0\n'
    expected_low = 'The Low across these days is  20.0\n'
    expected_rain = 'The Total snowfall across these days is  0.8 in.\n'
    expected_snow = 'The Total rainfall across these days is  0.09 in.\n'
                 
    assert expected_high in captured.out
    assert expected_low in captured.out
    assert expected_rain in captured.out
    assert expected_snow in captured.out

"""
Integrative Test to see if the UI can account for a date that is
incorrectly formatted
"""
def test_Historic_Inquiry_3_UIInput(capsys, monkeypatch):
    monkeypatch.setattr('sys.stdin', io.StringIO("Apr012021 - Apr022021\nexit\n"))
    UI = User_Interface_Historic()
    UI.Main()
    captured = capsys.readouterr()
    expected = 'Invalid input, try reinputting following the formatting guidelines'
    assert expected in captured.out

"""
Integrative Test to see if the UI can account for a random/incorrect text input
"""
def test_Historic_Inquiry_4_UIInput(capsys, monkeypatch):
    monkeypatch.setattr('sys.stdin', io.StringIO("blah\nexit\n"))
    UI = User_Interface_Historic()
    UI.Main()
    captured = capsys.readouterr()
    expected = "Invalid input, try reinputting following the formatting guidelines"
    assert expected in captured
"""
Integrative Test to see if the UI can account for a blank input
"""
def test_Historic_Inquiry_5_UIInput(capsys, monkeypatch):
    monkeypatch.setattr('sys.stdin', io.StringIO("\nexit\n"))
    UI = User_Interface_Historic()
    UI.Main()
    captured = capsys.readouterr()
    expected = "Invalid input, try reinputting following the formatting guidelines"
    assert expected in captured




