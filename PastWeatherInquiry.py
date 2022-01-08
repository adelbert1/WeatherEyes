import pymongo
from pymongo import MongoClient
import pprint
import datetime
from datetime import date

class User_Interface_Historic:
    """
    This class is designed to allow a user to inquire into a mongodb database that contains histoical data from weather.gov.
    Methods in this class will allow a user to inquire about a particular date or range of dates and return historical weather.gov
    data from a mongodb database
    """
    def __init__(self):
        self.dates = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        self.client = pymongo.MongoClient("mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        
    def PastWeather(self):
        """This method inquires into past weather information by submiting a particular date or range of dates.
        """ 
        db = self.client["historic"]
        collection = db["historic_dailyobs"]
        collection_hourly = db["historic_hourlyobs"]
        inquiry_type = input("Do you wish to make an inquiry into hourly information or daily information? Enter 'hourly' for hourly or 'daily' for daily information. ")
        if inquiry_type == 'daily':         
            dates = input("Please enter the date or range of dates you would like to return weather data for, (Jan 01 2021 - Jan 01 2022 format) if you want to select a single date, please enter a single date. Additionally, please note that the earliest date you can inquire into is Jun 12 2017 ")
            date_list = dates.split()
            Test_1 = len(date_list) == 3 and date_list[0] in self.dates and len(str(date_list[1])) == 2 and len(str(date_list[2])) == 4
            Test_2 = False
            if len(date_list) >= 7:
                Test_2 = len(date_list) == 7 and date_list[0] in self.dates and date_list[4] in self.dates and len(str(date_list[1])) == 2 and len(str(date_list[2])) == 4 and len(str(date_list[5])) == 2 and len(str(date_list[6])) == 4
            start_date = date_list[0] + " " + date_list[1] + " " + date_list[2]
            if Test_1:
                self.Single_Day_Inquiry(date_list, collection)
            if Test_2:
                end_date = date_list[4] + " " + date_list[5] + " " + date_list[6]
                print(date_list[0:3] , date_list[4:])
                self.Interval_Inquiry(date_list[0:3], date_list[4:], collection)
            if not Test_1 and not Test_2:
                print("Invalid input, try reinputting following the formatting guidelines")
        if inquiry_type == 'hourly':
            dates = input("Please enter the date or range of dates you would like to return weather data for, (Jan 01 2021 2 PM - Jan 01 2022 1 PM format) if you want to select a single date, please enter a single date. Additionally, please note that the earliest date you can inquire into is Jun 12 2017 ")
            date_list = dates.split()
            Test_1 = len(date_list) == 5
            Test_2 = len(date_list) == 11
            if Test_1:
                self.Single_Day_Inquiry_Hourly(date_list, collection_hourly)
            if Test_2:
                self.Interval_Inquiry_Hourly(date_list, collection_hourly)


    def Single_Day_Inquiry(self, date, collection):
        """This method inquires into a given datebase for a single date and prints the total
        rainfall, snowfall , high temperature and low temperature recorded on that date from weather.gov"""
        new_date = date[0]+date[1]+date[2]
        results = collection.find({"Date": new_date})
        for x in results:
            print("For the date of ", date[0], date[1], date[2])
            print("High Temperature: ", x['HighTempF'])
            print("Low Temperature: ", x['LowTempF'])
            print("Rainfall: ", x['Rainfall_i'], "in.")
            print("Snowfall: ", x['Snowfall_i'], "in.")

    def Single_Day_Inquiry_Hourly(self, date, collection):
        """
        This method is passed a date that includes a specified hour and a database for a single date and hour
        and prints the total rainfall, snowfall , high temperature and low temperature recorded on that date
        from weather.gov
        """
        Date = date[0] + date[1] + date[2]
        time = self.time_convert(date[3], date[4])
        results = collection.find({"Date":Date, "Time": time})
        for x in results:
            print("For the date of ", date[0], date[1], date[2], "at time ", time)
            print("The temperature was ", x['TemperatureF'])
            if x['Precipitation_i'] == 'No precipitation':
                print("There was no precipitation at this time")
            else:
                print("The precipitation was ", x['Precipitation_i'], "in.")

    def Interval_Inquiry_Hourly(self, date, collection):
        """This method inquires into a given database for a range of two dates which include specified hours.
        It then prints the total snow fall and rainfall between the two dates (inclusive) as well as the
        High and low temperature across the two dates and times (inclusive). It should be passed the date
        and hour interval as well as the database the data is stored in. All data from weather.gov"""
        start_date = date[0] + date[1] + date[2]
        start_time = self.time_convert(date[3], date[4])
        end_date = date[6] + date[7] + date[8]
        end_time = self.time_convert(date[9], date[10])
        temps = []
        rain = []
        delta = datetime.timedelta(days = 1)
        start = datetime.date(int(date[2]), self.recodedate_num(start_date[:3]), int(date[1]))
        end = datetime.date(int(date[8]), self.recodedate_num(end_date[:3]), int(date[7]))
        while start <= end:
            year = start.strftime('%Y')
            month = self.recodedate_str(start.strftime('%m'))
            day = start.strftime('%d')
            search_date = month + day + year
            switch = 0 
            while switch == 0:
                results = collection.find({"Date":search_date, "Time": start_time})
                for x in results:
                    temps.append(float(x['TemperatureF']))
                    if type(x['Precipitation_i']) == float or x['Precipitation_i'].isdigit():
                        rain.append(float(x['Precipitation_i']))
                start_time = self.iterate_time(start_time)
                if start_time == "1:00AM":
                    switch = 1
            switch = 0
            start = start + delta
        print("From", start_date, start_time, "To", end_date, end_time)
        print("The Total Precipitation over these days was ", sum(rain), "in.")
        print("The High over these days was ", max(temps))
        print("The Low over these days was ", min(temps))
        
    
    def Interval_Inquiry(self, start_date, end_date, collection):
        """This method inquires into a given database for a range of two dates. It then prints
        the total snow fall and rainfall between the two dates (inclusive) as well as the High
        and low temperature across the two dates (inclusive). All data from weather.gov"""
        #start_date_list = start_date.split()
        #end_date_list = end_date.split()
        start = date(int(start_date[2]), self.recodedate_num(start_date[0]), int(start_date[1]))
        end = date(int(end_date[2]), self.recodedate_num(end_date[0]), int(end_date[1]))
        delta = datetime.timedelta(days = 1)
        temps = []
        snow = []
        rain = []
        while start <= end:
            year = start.strftime('%Y')
            month = self.recodedate_str(start.strftime('%m'))
            day = start.strftime('%d')
            new_date = month+day+year
            results = collection.find({"Date": new_date})
            for x in results:
                temps.append(float(x['HighTempF']))
                temps.append(float(x['LowTempF']))
                if type(x['Rainfall_i']) == float or x['Rainfall_i'].isdigit():
                    rain.append(float(x['Rainfall_i']))
                if type(x['Snowfall_i']) == float or x['Snowfall_i'].isdigit():
                    snow.append(float(x['Snowfall_i']))
            start = start + delta
        print("From ", start_date, "to ", end_date)
        print("The High across these days is ", max(temps))
        print("The Low across these days is ", min(temps))
        print("The Total snowfall across these days is ", sum(snow), "in.")
        print("The Total rainfall across these days is ", sum(rain), "in.")

    def recodedate_num(self, date):
        """This method takes the name of a month and returns the numerical codification of the month"""
        recoding_num = {
            'Jan' : 1,
            'Feb' : 2,
            'Mar' : 3,
            'Apr' : 4,
            'May' : 5,
            'Jun': 6,
            'Jul' : 7,
            'Aug' : 8,
            'Sep' : 9,
            'Oct' : 10,
            'Nov' : 11,
            'Dec' : 12,
            }
        return recoding_num[date]
    
    def recodedate_str(self, date):
        """This method takes a numerical month and returns the actual name of the month as a string"""
        recoding_str = {
            '01' : 'Jan',
            '02' : 'Feb',
            '03' : 'Mar',
            '04' : 'Apr',
            '05' : 'May',
            '06' : 'Jun',
            '07' : 'Jul',
            '08' : 'Aug',
            '09' : 'Sep',
            '10': 'Oct',
            '11' : 'Nov',
            '12': 'Dec',
            }
        return recoding_str[date]

    def time_convert(self, time, ampm):
        """
        This method converts times into the format stored in mongodb from weather.gov. It is passed
        the time to convert and whether the time is in AM or PM
        """
        time = str(time) + ":00"
        if ampm == 'AM':
            time = str(time) + "AM"
        if ampm == 'PM':
            time = str(time) + "PM"
        return time

    def iterate_time(self, time):
        """
        This method is passed a time to increase by an hour. For instance 1 AM becomes 2 AM, 12 PM becomes
        1 AM.
        """
        if len(time) == 6:
            ampm = time[4:]
            lead = int(time[0])
        if len(time) == 7:
            ampm = time[5:]
            lead = int(time[0:2])
        lead = lead + 1
        if lead == 13:
            lead = 1
            if ampm == "AM":
                ampm = "PM"
            else:
                ampm = "AM"
        time = str(lead) + ":00" + ampm
        return str(time)


    def Main(self):
        """
        Calling this method will allow the user to input a date or range of dates to return historic weather
        data from weather.gov. Jan 01 2021 - Jan 01 2022 format for a range, Jan 01 2021 format for an individual
        date. Additionally, there is a restriction that only data from Jun 12 2017 onward is available.
        """
        UI = User_Interface_Historic()
        while True:
            UI.PastWeather()
            end = input("If you wish to enter a new input please hit enter, otherwise type 'exit' ")
            if end == "exit":
                self.client.close()
                break

if __name__ == "__main__":
    UI = User_Interface_Historic()
    UI.Main()
