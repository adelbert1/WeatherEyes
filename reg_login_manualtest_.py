#Test of the WeatherEyes log-in system using Selenium

import pytest
import time
from selenium import webdriver


def test_register(USERNAME, PASSWORD, EMAIL):
    driver = webdriver.Chrome('chromedriver.exe')
    driver.get('https://weathereyes-1.delberta.repl.co/register/') #The WeatherEyes registration website

    #Entering a Username
    user_name_input = driver.find_element_by_name('username')
    user_name_input.send_keys(USERNAME)

    #Entering an email
    email_input = driver.find_element_by_name('email')
    email_input.send_keys(EMAIL)

    #Entering a password
    password1_input = driver.find_element_by_name('password1')
    password1_input.send_keys(PASSWORD)

    #Confirming the password
    password2_input = driver.find_element_by_name('password2')
    password2_input.send_keys(PASSWORD)

    """
    Identifying the login button on the WeatherEyes login page
    and then clicking on it.
    """
    register_button = driver.find_element_by_xpath("//button[@class='btn btn-success']") 
    register_button.click()

def test_login(USERNAME, PASSWORD):
    driver = webdriver.Chrome('chromedriver.exe')
    driver.get('https://weathereyes-1.delberta.repl.co/login/') #The WeatherEyes login website


    """
    inputting the prescribed USERNAME into the login username field
    find user login required ID by 'inspect' in the web browser
    """
    user_input = driver.find_element_by_id('id_username')
    user_input.send_keys(USERNAME) 

    """
    inputting the prescribed PASSWORD into the login password field
    find user password ID by 'inspect' in the web browser
    """
    #password_input = driver.find_element_by_id('id_password')
    password_input = driver.find_element_by_name('password')
    password_input.send_keys(PASSWORD)

    """
    Identifying the login button on the WeatherEyes login page
    and then clicking on it.
    """
    login_button = driver.find_element_by_xpath("//button[@class='btn btn-success']") 
    login_button.click()

    time.sleep(1)

    welcome = driver.find_element_by_xpath('//*[@id="content"]/div/div/h2')
    assert("Welcome, "+ USERNAME + "!") in welcome.text

if __name__ == "__main__":
    USERNAME = 'test'
    PASSWORD = '!1testymctest1!'
    EMAIL = 'test.test@gmail.com'

    #test_register(USERNAME, PASSWORD, EMAIL)
    
    test_login(USERNAME, PASSWORD)
    
