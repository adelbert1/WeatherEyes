import urllib.request
from datetime import datetime
from datetime import date
import pymongo
from pymongo import MongoClient
from convert import Convert
"""
This class has functionality that allows one to visit a url containing historic
hourly weather observations and scrape relevant weather information.  The only
attribute this class currently has is a days attribute which is a dict where
all of the keys are months (01 is January, 09 is September, etc) and the values
associated with the keys are the number of days in that particular month. Note
that leap years are not considered in this dictionary, but are accounted for in
a later method
"""

class HistoricHourlyScraper:

    def __init__(self):
        self.days = {'01': '31',
                     '02': '28',
                     '03': '31',
                     '04': '30',
                     '05': '31',
                     '06': '30',
                     '07': '31',
                     '08': '31',
                     '09': '30',
                     '10': '31',
                     '11': '30',
                     '12': '31'}


    def convert_time(self, time):
        """ Converts 24-hour time to 12-hour time.  NOTE: This method
            automatically rounds up to the next nearest hour.  This
            implementation is intentional since the IEM site stores
            weather observations at --:51 for all times of the day.
            Rounding the hour up initially gives more peace of mind later
            rather than trying to adjust times in other methods.
            
            Args:
                time: string that indicates a particular time that an observation was collected
            Returns:
                string that an observation was recorded.  These return values will have
                the form hh:mm followed by AM or PM
            Raises:
                ValueError if time argument is a string but not formatted properly
        """
        try:
            if type(time) == str:
                split_time = time.split(':')
                hour = int(split_time[0]) + 1 # Round up to next hour
                if hour >= 12 and hour <= 23:
                    a_or_p = 'PM'
                else:
                    a_or_p = 'AM'
                mod_hour = str(hour%12)
                if mod_hour == '0':
                    mod_hour = '12'
                result = mod_hour + ':00' + a_or_p
                return result
            else:
                response = "You entered the wrong type of argument"
                print(response)
        except ValueError:
            print("It seems the time argument you passed is not formatted properly")
        
    def convert_date(self, date):
        """
        Splits date into preferred format.  Note that the date must be
        of the form yyyy-mm-dd time
        
        Args:
            date: date & time for weather observation, entered as a string.  From the
                historic hourly observation site, all dates are in the form yyyy-mm-dd
                followed by a space, and then the time that the observation was recorded.
                Note that month will be numeric, i.e. 01, 06, 11 and time is initially
                in 24-hour time
        Returns:
            a tuple containing stirng adaptation of date in the form mmddyyyy,
            with no spaces or separators, and the month is the first 3 letters
            of the name of the month in the first element of tuple and the time
            that the observation was recorded in the second element of the tuple
        Raises:
            AttributeError: date argument possibly of wrong type
            KeyError: month portion of the date argument did not have a valid key (something outside of 01 thru 12)
            IndexError: date argument was passed as a string but did not have the required format
        """
        #print('date', date)
        try:
            months_dict = {'01': 'Jan',
                           '02': 'Feb',
                           '03': 'Mar',
                           '04': 'Apr',
                           '05': 'May',
                           '06': 'Jun',
                           '07': 'Jul',
                           '08': 'Aug',
                           '09': 'Sep',
                           '10': 'Oct',
                           '11': 'Nov',
                           '12': 'Dec'}
            d = date.split('-') # split given date, using the dash ('-') as the separator
            #print(d)
            year = d[0]
            date_and_time = d[2]        # From historical observation site, this element will contain: day, blank space, then the time the observation was taken
            date_time_list = date_and_time.split()   # separate day from time from the third element of d
            # date_time_list variable will be list with day in first index and time in second index
            """
            The site that the historic observations are being pulled from updates each hour at --:51, for
            example there is an observation for 1:51, 2:51, ... ,11:51, 12:51.  Occasionally (once every
            few days), there will also be temperature/precip data input multiple times for one hour.  For the
            sake of simplicity, only pull data when the time is at 51 and round it up to the next nearest hour
            (i.e. 10:51 will be stored with time as 11).
            """
            #minute = y[1][3:5]
        
            time = self.convert_time(date_time_list[1])
            day = date_time_list[0]
            month_key = d[1]
            month = months_dict[month_key]
            result = (month + day + year, time)
            return result  
        except AttributeError:
            response = 'Make sure that the date you entered is passed as a string'
            print(response)
            #return response
        except (KeyError, IndexError):
            response = 'The date you entered was invalid'
            print(response)
            #return response
        
    def construct_dict(self, temp, when, precip):
        """
        Builds a dictionary with all relevant weather data

        Args: temp - tuple of F, C, and K temperatures for given observation
              when - date & time info for given observation; has form yyyy-mm-dd followed
                     by a space, and then has time of observation
              precip - amount of precipitation for given observation

        Returns:  Dictionary of relevant weather information

        Raises:
            TypeError if arguments are not passed in the proper format
        """
        try:
            date_stuff = self.convert_date(when) # Returns tuple with date & time info
            date = date_stuff[0]
            time = date_stuff[1]
            my_dict = {'Date': f'{date}',
                       'Time': f'{time}',
                       'TemperatureF': temp[0],
                       'TemperatureC': temp[1],
                       'TemperatureK': temp[2],
                       'Precipitation_i': precip[0],
                       'Precipitation_m': precip[1]}
            return my_dict
        except TypeError:
            print("Check that the when, temp, and precip argumetns are entered the arguments properly")

    def visit_url(self, url):
        """
        Uses urllib.request.urlopen to open specified url (provided that the status
        code received from this request is 200) and returns html document in
        raw bytes form. 

        Args:
            url- url of desired webpage; should be passed as a string

        Returns:
            list of raw data (in bytes) that was obtained from opening the
            requested url where each list element is a line from the web page

        Raises:
            AttributeError if url is not entered as a string.
            ValueError if url is entered as a string but is not a valid url
        """
        try:
            with urllib.request.urlopen(url) as page:
                #print(type(page))
                if page.status == 200:  # Should we handle other codes or is this okay?
                    #print('connecting...')
                    page = page.readlines() # Turns each line in webpage into an element in a list
                    return page
                else:
                    print(f'The url you tried visiting returned a {page.status} code')
        except ValueError:
            print('The url you tried visiting is not working.  Double-check your url')
        except AttributeError:
            print('Check that the url you tried visiting is formatted as a string')

    def extract_data(self, url):
        """
        Extracts relevant hourly weather observations from past data.  Can throw
        a ValueError if temp or precip are not recorded for an observation
        
        Args:
            url - url for webapage containing historical data to be scraped
        Returns:
            list of dictionaries that contain weather observation data
        Raises:
            Can raise ValueError if temp or precip are not entered for an observation
        """
        result = []
        page = self.visit_url(url)  # returns list where each list element is one line from the data in the webpage
        length = len(page)
        #print('length', length)
        my_range = range(length)
        for i in my_range:
            line = page[i].decode()# data is given in raw bytes, decode each element through each iteration
            line = line.split(',')    # split each line from webpage using comma (',') as the separator
            
            """
            Temperature is in third position of line and precipitation is in the
            eighth, so if either the third element of line or the eigth element of
            line is not empty, then proceed with collecting data observations
            """
            if line[2] != '' or line[7] !='':
                try:
                    tempF = float(line[2])  # Temperature data is only given once per hour and is either blank or has a temperature recording
                    tempC = Convert().FartoCel(tempF)
                    tempK = Convert().FartoKel(tempF)
                    temps =(tempF, tempC, tempK)
                    if line[7] == 'T':
                        precip_i = 'Trace amount'
                        precip_m = precip_i
                    elif line[7] == '0.00':
                        precip_i = 'No precipitation'
                        precip_m = precip_i
                    else:
                        precip_i = float(line[7])
                        precip_m = Convert().InchtoMM(precip_i)
                    precip = (precip_i, precip_m)
                    minute = line[1][14:16]    # Check that the minutes portion of time that observation was recorded is 51
                    if minute == '51':
                        when = line[1]         # Date and time when observation was recorded stored in 2nd index element
                        this_dict = self.construct_dict(temps, when, precip)
                        result.append(this_dict)
                    else:
                        pass
                except ValueError:
                    # Data could not be scraped because temp or precip could not be converted to floats
                    #print("Could not scrape this data point")
                    pass
        return result
            
        
    def get_today(self):
        """
        Retrieves the month, date, and year of the current day

        Args:
            None

        Returns:
            Tuple containing date information of the current day.  This tuple
            will have the following form: (month, day, year)

        Raises:
            This method should not raise any exceptions
        """
        today = str(date.today())       # Returns date in yyyy-mm-dd format
        # Get today's month, day, and year
        split_date = today.split('-')
        date_tup = (split_date[1], split_date[2], split_date[0])
        # date_tup is a tuple with (month, day, year) of today's date
        return date_tup

    def fetch_many_observations(self, test=False):
        """
        Fetches hourly weather data observations from January 1st, 2019 to
        previous day from IEM site.  Ideally this function should only be run
        once and then an hourly observation fetcher program will continuously
        pull the most recent observation.

        Args:
            test: argument that allows for quicker testing.  This argument
            has a default value of False, in which case all weather data
            starting from January 1st 2019 until current day will be pulled
            If the default value is changed to boolean True, then hourly
            weather data will be pulled from only January 1st 2019.  If this
            argument is anything else, return None and give the user a warning

        Returns:
            List of hourly weather data stored in dictionaries from January 1st
            2019 until previous day (meaning the day before this method is run)
            if test takes its default.  Otherwise, returns hourly weather data
            from January 1st 2019 only or None if test argument is invalid

        Raises:
            Should not raise any exceptions.  
        """
        
        """
        First, a url must be constructed based on the current date.  The urls
        for the historic hourly data have day1, year1, month1 and day2, year2,
        month2 attributes for starting and ending date ranges (1's are starting
        date, 2's are ending dates).  To do this, the current date must be
        obtained.  Also, note that month is numeric.  That is in the IEM url,
        month = 1 means January and month = 10 means October
        """
        if test == False:
            today_date = self.get_today()   # Returns ('mm', 'dd', 'yyyy')
        elif test == True:
            today_date = ('01', '01', '2019')
        else:
            print("Check that the test argument is either the boolean True or False")
            None
        end_day = today_date[1]
        #end_month = '10'
        end_month = today_date[0]
        end_year = today_date[2]
        if end_month[0:1] == '0':       # If month has a leading 0, remove it
            end_month = end_month[1:]
            #print(end_month)
        iem_url = f'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1=2019&month1=1&day1=1&year2={end_year}&month2={end_month}&day2={end_day}&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2'        
        weather_data = self.extract_data(iem_url)
        #for i in range(len(weather_data)):
            #print(weather_data[i])
        return weather_data


    def get_today_weather(self):
        """ 
        Fethches current day weather observations

        Args:
            None

        Returns:
            List of dictionaries where each dict is an hourly observation
            from the current day

        Raises:
            Should not raise any exceptions
        
        """
        
        today_date = self.get_today()   #today_date = ('mm', 'dd', 'yyyy')
        # Note that start and end date must be the same when fetching hourly observations of a single day
        day = today_date[1]
        month = today_date[0]
        year = today_date[2]
        if month[0:1] == '0':       # If month has a leading 0, remove it
            month = month[1:]
        iem_url = f'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1={year}&month1={month}&day1={day}&year2={year}&month2={month}&day2={day}&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2'        
        weather_data = self.extract_data(iem_url)
        """for i in range(len(weather_data)):
            print(weather_data[i])"""
        return weather_data

    def get_past_hour(self):
        """
        Fetches most recent weather observation (almost always most previous hour)

        Args:
            None

        Returns:
            Dictionary of most recent hourly observation

        Raises:
            -TypeError if data being added to Mongo is not a dictionary, if
                db_name is not a string, or if url is not a string
            -ServerSelectionTimeoutError if url is a string, but not a url
            -InvalidURI if url is valid for some website but does not begin with
                'mongodb+srv'
            
        """
        #now = datetime.now('ETS')
        #obs_today = self.get_today_weather()
        obs_today = []
        if len(obs_today) > 0:  # There was at least one observation recorded for today
            return obs_today[-1]

        else:                   # No observations for today, go back to previous day
            today_date = self.get_today()
            #today_date = ('01','01','2021')
            #today_date = ('03', '01', '2021')
            day = today_date[1]
            month = today_date[0]
            if int(today_date[2])%4==0:  # Leap year! Change the value associated with February key (02) in days attribute of the class 
                self.days['02'] = '29'
                
            if day == '01' and month =='01':        # January first, yesterday is December 31st
                day = '31'
                month = '12'
                year = str(int(today_date[2])-1)
                iem_url = f'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1={year}&month1={month}&day1={day}&year2={year}&month2={month}&day2={day}&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2'        
                weather_data = self.extract_data(iem_url)
            elif day == '01' and month != '01':     # Start of a new month, yesterday is last day of previous month
                year = today_date[2]
                month = int(month)-1
                if month <= 9:
                    month = '0' + str(month)
                else:
                    month = str(month)
                print('month', month)
                day = self.days[month]
                iem_url = f'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1={year}&month1={month}&day1={day}&year2={year}&month2={month}&day2={day}&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2'        
                weather_data = self.extract_data(iem_url)
            else:
                year = today_date[2]
                iem_url = f'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1={year}&month1={month}&day1={day}&year2={year}&month2={month}&day2={day}&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2'        
                weather_data = self.extract_data(iem_url)
        return weather_data[-1]
 
    def write_many_mongo(self, url, db_name):
        """
        Calls weather scraping function and writes many observations to MongoDB

        Args:
            url - client url to connect to MongoDB
            db_name - name of the database that an observation is to be added to 

        Returns:
            None, but does write weather observations into shared database
            
        Raises:
            -TypeError if data being added to Mongo is not a dictionary, if
                db_name is not a string, or if url is not a string
            -ServerSelectionTimeoutError if url is a string, but not a url
            -InvalidURI if url is valid for some website but does not begin with
                'mongodb+srv'
        """
        #my_url = "mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
        cluster = pymongo.MongoClient(url)
        db = cluster[db_name]
        collection_hourly = 'historic_hourlyobs'
        collection = db[collection_hourly]
        weather_data = self.fetch_many_observations()   # Pulls weather observations
        collection.insert_many(weather_data)

    def write_hourly_mongo(self, url, db_name):
        """
        Writes most recent weather observation into MongoDB

        Args:
            url - client url to connect to MongoDB
            dn_name - name of the database that an observation is to be added to 

        Returns:
            None, but does write weather observations into shared database
            
        Raises:

            
        """
        cluster = pymongo.MongoClient(url)
        db = cluster[db_name]
        collection_hourly = 'historic_hourlyobs'
        collection = db[collection_hourly]
        recent_obs = self.get_past_hour()
        collection.insert_one(recent_obs)
        
        
                
        
        
            
if __name__ == '__main__':
    """
    sample_url contains historic hourly observations for a April 1st 2021
    to April 9th 2021 and hourly_url contains historic hourly observations
    for June 1st 2017 to April 9th 2021.  The hourly_url has a great deal
    more data, but data is stored in the same format for both url's.  I
    used sample_url to get this method working and so that it could be
    applied to a much larger data set but still be effective. 
    """
    h = HistoricHourlyScraper()
    #h.fetch_many_observations()
    #print(h.get_today_weather())
    #print(h.get_past_hour())

    my_mongo_url = "mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    db_name = "historic"
    h.write_many_mongo(my_mongo_url, db_name)
    
    #h.write_hourly_mongo(my_mongo_url, db_name)
    #print(h.get_past_hour())
    



"""
http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1=2020&month1=12&day1=31&year2=2020&month2=12&day2=31&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2
http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=PIT&data=all&year1=2020&month1=12&day1=31&year2=2020&month2=12&day2=31&tz=America%2FNew_York&format=onlycomma&latlon=no&elev=no&missing=empty&trace=T&direct=no&report_type=1&report_type=2
    """
