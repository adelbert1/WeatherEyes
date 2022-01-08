import urllib.request
from convert import Convert
from bs4 import BeautifulSoup
from datetime import date
import pymongo
from pymongo import MongoClient

class HistoricScraper:
    
    """
    This class scrapes weather.gov historical weather data pages and pulls
    max & min temps, snow, and rainfall for each day starting from June 2017 to
    the present (April 2021, as I write this).  One thing to note is that the
    only method that needs to be manually called (via a if __name__=='__main__'
    block) is the scraper() method.  All other methods are called in the
    scraping process by scraper().

    All data tables from the weather.gov histroical site are set up the exact same
    way, which makes extracting the data as simple as being able to deal with
    python strings and lists and methods pertaining to these data types.  A sample
    web page to be scraped can be found by visiting the below url:

    https://forecast.weather.gov/product.php?site=NWS&issuedby=PIT&product=CF6&format=CI&version=14&glossary=0

    """

    
    def __init__(self):
        pass
                 
    def visit_url(self, url):
        """
        Uses urllib.request.urlopen to open specified url (provided that the status
        code received from this request is 200) and returns html document in
        raw bytes form.  Can throw a ValueError if the url is invalid (not returning
        a 200 code) and can also throw an AttributeError if url is not entered as a
        string.

        Args:
            url- string representation of url of desired webpage

        Returns:
            raw data (in bytes) that was obtained from opening the requested url
            if status code is 200.  Otherwise, return None
        Raises:
            ValueError if url argument is invalid but still in passed as a string
            AttributeError if url argument is not a string
        """
        try:
            with urllib.request.urlopen(url) as page:
                #print(type(page))
                if page.status == 200:  # Should we handle other codes or is this okay?
                    #print('connecting...')
                    page = page.read()
                    return page
                else:
                    print(f'The url you tried visiting returned a {page.status} code')
                    return None
        except ValueError:
            print('The url you tried visiting is not working.  Double-check your url')
        except AttributeError:
            print('Check that the url you tried visiting is formatted as a string')

    def get_month(self, header):
        """
        Pass header information from data table as a list so that month/year pair
        can be extracted and returned.  Note that list of header information (obtained
        in main body of weather() method) should always have length 37(except Jan.
        Feb. of 2020, they have an extra element in their header. Can throw a TypeError
        if header is not passed as a list

        Args:
            header: header information from scraped web page(Contains date info)
                    and should be passed as a list

        Returns:
            a tuple where the first element is the month of weather
            observations and second element is the corresponding year

        Raises:
            TypeError if header is not passed as a list
            KeyError if date information passed has invalid month key
        """
        try:
            months_dict = {'JANUARY': 'Jan',
                           'FEBRUARY': 'Feb',
                           'MARCH': 'Mar',
                           'APRIL': 'Apr',
                           'MAY': 'May',
                           'JUNE': 'Jun',
                           'JULY': 'Jul',
                           'AUGUST': 'Aug',
                           'SEPTEMBER': 'Sep',
                           'OCTOBER': 'Oct',
                           'NOVEMBER': 'Nov',
                           'DECEMBER': 'Dec'}
            #print(len(header))
            if len(header) == 37:       # Normal header
                month = months_dict[header[16]]
                year = header[18]
                when = (month, year)    
                return when
            elif len(header) == 38:     # Header has an extra element
                month = months_dict[header[17]]
                year = header[19]
                #print(when)
                when = (month, year)
                return when
            else:
                print('Header was passed as a list but has the wrong length')
        except (TypeError, KeyError):
            print('It seems the argument you tried passing was invalid')

    def table_splitter(self, table, delim):
        """
        Splits data table from scraped web page using Python's built-in split()
        function.  The argument to split() is the delimeter (string of equals signs
        used in the table on the web page beings scraped. Note: for the weather.gov
        historical data, this should ALWAYS return a list with 6 elements (all hist-
        orical weather observation tables have the same format). Can give a TypeError
        if table is not passed as a string.

        Args:
            table - string representation of historical weather observations
                table scraped from weather.gov web page
            delim - delimeter used on the web page to orient table

        Returns:
            list of elements from the table after splitting on the given delimeter.

        Raises:
            TypeError if table or delim  args are not passed as strings
        """
        try:
            split_table = table.split(delim)
            return split_table
        except TypeError:
            print("I think you tried using the wrong type of data.")
            
    def scrape_ify(self, data, when):
        """
        Builds a dictionary with relevant weather data information for a single month

        Args:
            data - a single row of weather observations from the data table, entered
              as a string!
            when - a 2-tuple where the first element is the month and second
                element is the year of corresponding data table (get_month
                function returns a tuple of this form)

        Returns:
            dictionary containing relevant scraped weather information

        Raises:
            TypeError if date infomation is wrong or weather information was
            input incorrectly in the table.  If the latter is the case, the
            observation will just be skipped (though I don't think this
            is a problem I have encountered thus far
        """
        try:
            # Weather info we are concerned about should always be in the same spot
            # for any row of the data table
            split_data = data.split()
            #print(split_data)
            day = int(split_data[0])          # Date info will always be in first index
            if day < 10:
                day = '0' + str(day)
            max_tempF = float(split_data[1])     # Max temp always in second position
            min_tempF = float(split_data[2])     # Min temp is in third position
            max_tempC = Convert().FartoCel(max_tempF)
            min_tempC = Convert().FartoCel(min_tempF)
            max_tempK = Convert().FartoKel(max_tempF)
            min_tempK = Convert().FartoKel(min_tempF)
            """ 
            Rainfall is in 8th index and Snow is in the 9th.  If the value
            for either of these indices is 'T', then there was a trace amount
            of that type of precipitation recorded. Similarly, if the value for
            a particular entry in the table is 'M', then that data value is missing.
            Otherwise, these values should be strings of float-like data (numbers with
            decimal points).
            """
            T = 'Trace amount'      
            M = 'Missing information'
            
            if split_data[7] == 'T':        # Check rainfall for numeric data, missing data, or trace amounts
                rain_i = T
                rain_m = T
            elif split_data[7] == 'M':
                rain_m = M
                rain_i = M
            else:
                rain_i = float(split_data[7])
                rain_m = Convert().InchtoMM(rain_i)

            if split_data[8] == 'T':        # Check snowfall for numeric data, missing data, or trace amounts
                snow_i = T
                snow_m = T
            elif split_data[8] == 'M':
                snow_i = M
                snow_m = M
            else:
                snow_i = float(split_data[8])
                snow_m = Convert().InchtoMM(snow_i)
            
            month = when[0]
            year = when[1]
            date = month + str(day) + year
            weather_obs = {'Date': f'{date}',
                           'HighTempF':max_tempF,
                           'LowTempF':min_tempF,
                           'HighTempC':max_tempC,
                           'LowTempC': min_tempC,
                           'HighTempK': max_tempK,
                           'LowTempK': min_tempK,
                           'Rainfall_i':rain_i,
                           'Rainfall_m': rain_m,
                           'Snowfall_i': snow_i,
                           'Snowfall_m': snow_m}
            return weather_obs
        except TypeError:
            print('uh oh, one of your arguments was of the wrong type')
                
    def weather_extracter(self, url):
        """
        Scrapes raw daily historical weather data from weather.gov and then
        uses the scrape_ify function to build a dictionary of weather observations
        for each day in a given month 

        Args:
            url: weather.gov url that contains weather data to be extracted

        Returns:
            List of dictionaries of weather data for a single month

        Raises:
            urllib.error.URLError if url is invalid
            AttributeError is raised when a url for a non-weather.gov site is entered
            
        """
        
        past_obs_list = []
        obs = {}
        day = []
        max_temp = []
        min_temp = []
        rain = []
        snow = []
        visitor = self.visit_url(url)
        #print(visitor)
        soup = BeautifulSoup(visitor, 'html.parser')
        table = soup.find('pre')    # 'pre' tag is where data table is on when page
        table_text = table.get_text()
        delim = '================================================================================'
        split_table = self.table_splitter(table_text, delim)
        
        header = split_table[0].split() # Split header from table to extract date information
        when = self.get_month(header)   # Recall get_month function returns a tuple
        #print("WHEN:", when)
            
        data =  split_table[2]          # Data table that contains weather observations in this element
        split_data = data.splitlines()  # Break data table into lists of row elements
        split_data = split_data[2:]     # First two lines are empty string; remove them
        
        """
        Calling splitlines on the data table allows access to each row of the data,
        which is important when trying to extract necessary information.  The
        WX column (16th column) may be empty in the original data table which will
        not be reflected in the split_data (the table skips right to the 17th
        column if there is no data in WX column) but this should not matter since
        the information we are concerned about is in the first 8 columns.
        """
        
        length = len(split_data)
        ranger = range(length)
        #print("CHECK THESE!", length, ranger)
        
        # Extract relevant weather infomation from the data table

        for i in ranger:
            obs = self.scrape_ify(split_data[i], when)
            past_obs_list.append(obs)
        return past_obs_list

    def scraper(self):
        """
        This function is the driving force that scrapes all 50 weather.gov pages
        of historical data by calling the weather_extracter function, and is the
        only function that needs to be called in order successfully scrape weather
        data.  Note that the urls for the weather.gov historical data pages
        differ only in one spot, and that is in the version='' attribute within
        the url.  Each version is a number between 1 and 50 and corresponds to months
        of weather observations (i.e. April 2021 is version 1, March 2021 is version 2,
        ... July 2017 is version 49, and June 2017 is version 50).

        Args: None

        Returns: 
            Nested list of dictionaries for each day that contain weather
            observation data across all available months.  Each day's
            weather observations will be in a dictionary, a single
            month of these observation dictionaries will be a list,
            and each of the 47 months worth of data is then stored
            in another list 

        Raises:
            Any exceptions raised will be thrown by the weather_extracer function
        
        """
        save_data = []
        url_version = [(i+1) for i in range(50)]    # weather.gov urls for historic data differ only by their version attribute in the actual url
        """
        Oddly enough, the weather.gov historical weather site has duplicate
        web page data tables for February, March, and April of 2019.  To
        avoid overscaping, skip over these pages when extracting the data. Also,
        Febraury of 2018 data table is not functioning properly, so we will
        skip that for now
        """
        url_version.remove(25)
        url_version.remove(27)
        url_version.remove(29)
        url_version.remove(42)
        urls = []
        #print(url_version)
        ranger = range(len(url_version))
        for i in ranger:
            """Obtains 47 different versions (urls) for the 47 months of
            data table by calling weather_extracter on each of the urls
            in url_version.  Recall that weather_extracter"""
            v = url_version[i]      # This allows one to go through the different months of data by using a string formatting as below
            my_url = f'https://forecast.weather.gov/product.php?site=NWS&issuedby=PIT&product=CF6&format=CI&version={v}&glossary=0'
            urls.append(my_url)
            result = self.weather_extracter(urls[i]) # Scrapes data from all 47 months
            #print(result)
            save_data.append(result)
        #print("All done scraping :3")
        return save_data

    def pretty_print(self, data, length):
        """
        Prints data values from a nested list so that one element is output
        per line.

        Args:
            data: Nested list of data whose elements are to be printed.  
            length: integer number of elements of the data argument to be printed.  If length
                    argument is longer than the length the data argument, then the method
                    breaks and a warning message is dispayed

        Returns:
            None, but does give a good output of data that is (semi) easy to read

        Raises:
            TypeError is raised when arguments are not of the right type
        """
        len_data = len(data)
        #print(len_data)
        if len_data < length:       # Length argument is greater than length of the list, print warning
            print('The length you requested was longer than the length of the list')
            valid = False
        else:                       # Length argument is okay
            valid = True
        while valid:
            ranger = range(length)
            for i in ranger:
                if type(data[i]) is list:
                    inside_len = len(data[i])
                    inner_ranger = range(inside_len)
                    #print(inner_ranger)
                    for j in inner_ranger:
                        print(data[i][j])
                else:
                    print("The pretty_print method only works for nested lists, and it seems like you tried passing something else to this method")
            print('\n')                 # Newline after method finishes running    
            valid = False               # Update loop condition

    def weather_yester_data(self):
        """
        Fetches weather observations from previous day

        Args:
            None

        Returns:
            Dictionary of previous day weather observations

        Raises:
            As long as the datetime module from Python does not change, this method
            should not raise any errors
        
        """
        today = int(str(date.today())[8:10])
        #today = 1
        if today == 1:      # Today is the start of a new month.  Yesterday was the last day of the previous month
            v = 2           # Need to visit version 2, which is weather info from last month
            my_url = f'https://forecast.weather.gov/product.php?site=NWS&issuedby=PIT&product=CF6&format=CI&version={v}&glossary=0'
        else:               # Not at the beginning of a new month, so just go to current month's observations
            v = 1
            my_url = f'https://forecast.weather.gov/product.php?site=NWS&issuedby=PIT&product=CF6&format=CI&version={v}&glossary=0'
        observations = self.weather_extracter(my_url)   # This will be a list of dictionaries
        yester_data = observations[-1]
        return yester_data

    def write_many_mongo(self, url, db_name):
        """
        Calls weather scraping function and writes many observations to MongoDB

        Args:
            url - client url to connect to MongoDB
            db_name - name of the database that observations are to be added to 

        Returns:
            None, but does write weather observations into shared database
            
        Raises:
            -TypeError if data being added to Mongo is not a dictionary, if
                db_name is not a string, or if url is not a string
            -ServerSelectionTimeoutError if url is a string, but not a url
            -InvalidURI if url is valid for some website but does not begin with
                'mongodb+srv'
        
        """
        cluster = pymongo.MongoClient(url)
        db = cluster[db_name]
        collection_daily = 'historic_dailyobs'
        collection = db[collection_daily]
        weather_data = self.scraper()       # Pulls weather observations
        ranger = range(len(weather_data))
        for i in ranger:
            collection.insert_many(weather_data[i])

    def write_daily_mongo(self, url, db_name):
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
        cluster = pymongo.MongoClient(url)
        db = cluster[db_name]
        collection_daily = 'historic_dailyobs'
        collection = db[collection_daily]
        recent_obs = self.weather_yester_data()
        collection.insert_one(recent_obs)
        

           
if __name__ == "__main__":
    h = HistoricScraper()
    my_mongo_url = "mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    db_name = "historic"
    h.write_many_mongo(my_mongo_url, db_name)
    #data = h.scraper()
    #h.pretty_print(data, len(data)) 
    #h.write_many_mongo('https://www.duq.edu', db_name) Test
    


    

    """   
    #history = scraper()
    #print(history[10][2].items())
    #print(history[10][2]['Month'])
    #my_test = h.visit_url('https://www.duq.edu')
    #another_test = h.visit_url(14)
    #test_url = h.visit_url('trunk') # returns ValueError
    
    h = HistoricScraper()
    
    bad_weather_url = 'htt://forecast.weather.gov/product.php?site=NWS&issuedby=PIT&product=CF6&format=CI&version=14&glossary=0'
    duq = 'https://www.duq.edu'
    #extraction = h.weather_extracter(duq) Gives attribute error
    #print(extraction)

    data = h.scraper()
    h.pretty_print(data, 4)
    #h.convert_temp(85.0)
    
    
    print("Yesterday's weather observations:", h.weather_yester_data())
    
    #print(data[1])
    #print(len(data[1]))
    #for i in range(len(data)):
        #print(f'{i}:', data[i][0])
    #print(data[10][2]['Month']=='Jun')
weather_obs = {'Date': f'{date}',
                           'HighTempF':f'{max_tempF}',
                           'LowTempF':f'{min_tempF}',
                           'HighTempC':f'{max_tempC}',
                           'LowTempC': f'{min_tempC}',
                           'HighTempK': f'{max_tempK}',
                           'LowTempK': f'{min_tempK}',
                           'Rainfall':f'{rain}',
                           'Snowfall': f'{snow}'}
    
"""
