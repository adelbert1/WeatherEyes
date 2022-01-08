from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import urllib.request
import pymongo
from pymongo import MongoClient
from datetime import datetime
from convert import Convert


#This disctionary takes a numerical month and converts the actual name of the month as a string.
num_to_month = {1:"Jan", 2:"Feb", 3:"Mar", 
                4:"Apr", 5:"May", 6:"June", 
                7:"July", 8:"Aug", 9:"Sep", 
                10:"Oct", 11:"Nov", 12:"Dec"}

digit2 = {"1":"01",
          "2":"02",
          "3":"03",
          "4":"04",
          "5":"05",
          "6":"06",
          "7":"07",
          "8":"08",
          "9":"09",}
"""
The script web-scrapes the AccuWeather.com website to extract hourly forecast and daily forecst. 
The weather information this class fetches information includes max temperature, min temperature, precipitation probability, and weather forecast description.
A web page to be scraped can be found by visiting the below url:
https://www.accuweather.com/en/us/pittsburgh/15282/hourly-weather-forecast/1310?day
and
https://www.accuweather.com/en/us/pittsburgh/15282/daily-weather-forecast/1310
"""

class AccuWeather_forecast:
    def __init__(self):
        self.zipcode = "15231" #using Duquesne zip code as the default for the data we're collecting.
        self.hourly_data = []
        self.daily_data = []
        self.now = datetime.now() #using datetime module in order to get the current time
        self.convert = Convert()
        
   
    
    def change_zipcode(self, zipcode):
        """  
        Change the zipcode attribute to the desired zipcode. 
        """ 
        self.zipcode = zipcode
 
    def hourly_forecast(self):
        """  
        Extracting the 24 hours hourly forecast data from Accuweather.com
        """ 
        zipcode = self.zipcode
        url_day1 = f'https://www.accuweather.com/en/us/pittsburgh/{zipcode}/hourly-weather-forecast/1310?day'
        url_day2 = f"https://www.accuweather.com/en/us/pittsburgh/{zipcode}/hourly-weather-forecast/1310?day=2"
        req_day1 = Request(url_day1, headers={'User-Agent': 'XYZ/3.0'})
        req_day2 = Request(url_day2, headers={'User-Agent': 'XYZ/3.0'}) 
        response_day1 = urlopen(req_day1, timeout=20).read()
        response_day2 = urlopen(req_day2, timeout=20).read()
        soup_day1 = BeautifulSoup(response_day1, features = "html.parser")
        soup_day2 = BeautifulSoup(response_day2, features = "html.parser")
        num_hour_day1 = len(soup_day1.find_all('div', class_="hourly-card-nfl-header"))
        num_hour_day2 = len(soup_day2.find_all('div', class_="hourly-card-nfl-header"))

        data_list_day1 = []
        for i in range(num_hour_day1):
            temp_dic_day1 = {}
            hour1 = soup_day1.find_all('h2', class_="date")[i].get_text().replace("\n", "").split("\t")[0].split()
            temp_dic_day1["Hour"] = hour1[0]+hour1[1]
            date1 = soup_day1.find_all('h2', class_="date")[i].get_text().replace("\n", "").split("\t")[3]
            lst_temp = date1.split("/")
            
            if int(lst_temp[1])<10:
                temp_dic_day1["Date"] = str(num_to_month[int(lst_temp[0])])+digit2[lst_temp[1]]+str(self.now.year)
            else:
                temp_dic_day1["Date"] = str(num_to_month[int(lst_temp[0])])+lst_temp[1]+str(self.now.year)
            
            temp_dic_day1["PrecipitationProb"] = int(soup_day1.find_all('div', class_="precip")[i].get_text().replace("\n", "").replace("\t", "")[:-1])
          
            temp_dic_day1["TemperatureF"] = int(soup_day1.find_all('div', class_="temp")[i].get_text()[3:5])
            self.temp = temp_dic_day1["TemperatureF"]
            temp_dic_day1["TemperatureC"] = self.convert.FartoCel(self.temp)
            temp_dic_day1["TemperatureK"] = self.convert.FartoKel(self.temp)
            temp_dic_day1["Description"] = soup_day1.find_all('span', class_="phrase")[i].get_text().replace("\n", "").replace("\t","")
            date_time = self.now.strftime("%b%d%Y %I%p")
            temp_dic_day1["TimeCollected"] = date_time
            data_list_day1.append(temp_dic_day1)

        data_list_day2 = []
        for i in range(num_hour_day2):
            temp_dic_day2 = {}

            hour2 = soup_day2.find_all('h2', class_="date")[i].get_text().replace("\n", "").split("\t")[0].split()
            temp_dic_day2["Hour"] = hour2[0]+hour2[1]
            date2 = soup_day2.find_all('h2', class_="date")[i].get_text().replace("\n", "").split("\t")[3]
            lst_temp = date2.split("/")
            
            if int(lst_temp[1])<10:
                temp_dic_day2["Date"] = str(num_to_month[int(lst_temp[0])])+digit2[lst_temp[1]]+str(self.now.year)
            else:
                temp_dic_day2["Date"] = str(num_to_month[int(lst_temp[0])])+lst_temp[1]+str(self.now.year)
            

            temp_dic_day2["PrecipitationProb"] = int(soup_day2.find_all('div', class_="precip")[i].get_text().replace("\n", "").replace("\t", "")[:-1])
            temp_dic_day2["TemperatureF"] = int(soup_day2.find_all('div', class_="temp")[i].get_text()[3:5])
            self.temp = temp_dic_day2["TemperatureF"]
            temp_dic_day2["TemperatureC"] = self.convert.FartoCel(self.temp)
            temp_dic_day2["TemperatureK"] = self.convert.FartoKel(self.temp)
            temp_dic_day2["Description"] = soup_day2.find_all('span', class_="phrase")[i].get_text().replace("\n", "").replace("\t","")
            date_time = self.now.strftime("%b%d%Y %I%p")
            temp_dic_day2["TimeCollected"] = date_time
            data_list_day2.append(temp_dic_day2)
        self.hourly_data = (data_list_day1+data_list_day2)[0:24]

    def daily_forecast(self):
        """  
        Extracting the 12 days daily forecast data from Accuweather.com
        """
        zipcode=self.zipcode
        url = f'https://www.accuweather.com/en/us/pittsburgh/15219/daily-weather-forecast/{zipcode}'
        req = Request(url, headers={'User-Agent': 'XYZ/3.0'})
        response = urlopen(req, timeout=20).read()
        soup = BeautifulSoup(response, features = "html.parser")
        #print(soup.prettify())

        data_list = []
        for i in range(10):
            temp_dic = {}
            temp_dic["Date"] = soup.find_all('span', class_="module-header sub date")[i].get_text()
            lst_temp = temp_dic["Date"].split("/")
            if int(lst_temp[1])<10:
                temp_dic["Date"] = str(num_to_month[int(lst_temp[0])])+digit2[lst_temp[1]]+str(self.now.year)
            else:
                temp_dic["Date"] = str(num_to_month[int(lst_temp[0])])+lst_temp[1]+str(self.now.year)
                
            
            temp_dic["MinTempF"] = int(soup.find_all('span', class_="low")[i].get_text()[1:3])
            self.mintempF= temp_dic["MinTempF"]
            temp_dic["MinTempC"] = self.convert.FartoCel(self.mintempF)
            temp_dic["MinTempK"] = self.convert.FartoKel(self.mintempF)
            
            temp_dic["MaxTempF"] = int(soup.find_all('span', class_="high")[i].get_text()[:2])
            self.maxtempF = temp_dic["MaxTempF"]
            temp_dic["MaxTempC"] = self.convert.FartoCel(self.maxtempF)
            temp_dic["MaxTempK"] = self.convert.FartoKel(self.maxtempF)
            temp_dic["Description"] = soup.find_all('div', class_="phrase")[i].get_text().replace("\n", "").replace("\t","")
            temp_dic["PrecipitationProb"] = int(soup.find_all('div', class_="precip")[i].get_text().replace("\n", "").replace("\t", "")[:-1])
            date_time = self.now.strftime("%b%d%Y %I%p")
            temp_dic["TimeCollected"] = date_time
            data_list.append(temp_dic)
        self.daily_data = data_list

    def write_to_mongodb(self, url, collection_name):
        """  
        This method is to write the data we collect into MongoDB database.
        """
        # url = "mongodb+srv://hoy:1234@cluster0.ek3ux.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
        cluster = MongoClient(url)
        # collection_name = "test"
        collection_name_hour = collection_name + "_hourly"
        collection_name_daily = collection_name + "_tenday"
        db = cluster[collection_name]
        collection_hour = db[collection_name_hour]
        collection_daily = db[collection_name_daily]
        ## save the data onto MongoDB
        collection_hour.insert_many(self.hourly_data)
        collection_daily.insert_many(self.daily_data)
        
if __name__=="__main__":
    class_obj = AccuWeather_forecast()
    zipcode = "15231" ## change this line to your desired zipcode if necessary
    #class_obj.change_zipcode(zipcode)
    class_obj.hourly_forecast()
    class_obj.daily_forecast()
    #change the next two lines for saving the data into another MongoDB database collection
    mongo_url = "mongodb+srv://hoy:1234@cluster0.ek3ux.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    collection_name = "test"
    class_obj.write_to_mongodb(mongo_url, collection_name)
#   print(class_obj.hourly_data)
#   print(class_obj.daily_data)
