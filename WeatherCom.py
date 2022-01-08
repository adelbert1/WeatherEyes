from convert import Convert
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import datetime 
import time
import pymongo
from pymongo import MongoClient


class WeatherCom:
    
    """
    
    WeatherCom is a class to extract the date from weather.com in HTML format
    and transform the data into a dictionary to store them in the mongodb database.
    
    We have used multimultiple python library such as requests, BeautifulSoup, datetime,time and MongoClient
    - requests: it is a liberary that is used to allows the code to send HTTP/1.1 requests in simple way.
    - BeautifulSoup: this liberary is used to extract the data out from the html file
    - datetime, time: both are used to manipulate dates and times
    - MongoClient: it is a liberary which is used to write to MongoDB
    
    """
    
    def __init__(self):
        
        """ 
        
        we use the function now() from datatime module to get current time and date and store 
        it as a global variable in self.now
        
        The URL has fieldset which is return the forecast
        of Pittsburgh Airport weather in Fehrenhite. 
        
        The data that we have extract from the URL is stored into the response variable -page-. This code line
        has been repeated three times with three different timestep: today, tenday, and hourbyhour forecast. 
        
        We have used BeautifulSoup to parse html content of the page object, so the data can easliy 
        extracted from HTML. The prased HTML pags is stored in soup object. 
        
        The function importInformation(soup, timeStep) is called three times for three different timeStep,
        for each time we pass the soup and the timeStep as parameter to return the information that we 
        have asked for in a list. All three list is a global variable.        . 
        
        The last step that the function sleep has been used because one of the Weather.com restrictions 
        it is not allowed to access the website within 10 second 
        
        """
        
        self.now = datetime.datetime.now()    

        self.convert = Convert()
        
        timeStep="today"        
        page = requests.get("https://weather.com/weather/"+ timeStep +"/l/Pittsburgh+International+Airport+PIT:9:US")
        soup=BeautifulSoup(page.content,"html.parser")
        self.today_list = self.importInformation(soup, timeStep)
        time.sleep(10)
        #print(self.today_list)
        
        timeStep="tenday"
        page = requests.get("https://weather.com/weather/"+ timeStep +"/l/Pittsburgh+International+Airport+PIT:9:US")
        soup=BeautifulSoup(page.content,"html.parser")
        self.tenday_list = self.importInformation(soup, timeStep)
        time.sleep(10)
        #print(self.tenday_list)

        timeStep ="hourbyhour" 
        page = requests.get("https://weather.com/weather/"+ timeStep +"/l/Pittsburgh+International+Airport+PIT:9:US")
        soup=BeautifulSoup(page.content,"html.parser")
        self.hourly_list = self.importInformation(soup, timeStep)
        time.sleep(10)
        #print(self.hourly_list)
        
        
    def importInformation(self, soup, timeStep):
        """
        
        The function importInformation(soup, timeStep) is called to extract the required attribute from the HTML tags.
        This function has three cases, for today information, tenday information and hourly information. Each case of those
        three cases has its own HTML tags, so the tag that we have extract the Max_temp (in F) for today time step is 
        different than the one that is used to extract Max_temp (in F) of tenday time step. The result is returned in a list 
        of dictionay where each dictionary has (day, Temp, Max_temp (in F), Min_temp (in F), precipitationProb, Description
        and Time collected). However, the hourly dictionary has one more attribute which is (hour). 
        
        """

        if timeStep=="today":
            l=[]
            d = {}
            d["Date"]=self.now.strftime("%b%d%Y")
            d["TemperatureF"] = int(soup.find_all("span",{"class":"CurrentConditions--tempValue--3KcTQ", "data-testid":"TemperatureValue"})[0].text[:-1])
            d["TemperatureC"] = self.convert.FartoCel(d["TemperatureF"])
            d["TemperatureK"] = self.convert.FartoKel(d["TemperatureF"])
            d["Description"] = soup.find_all("div",{"class":"CurrentConditions--phraseValue--2xXSr", "data-testid":"wxPhrase"})[0].text
            d["TimeCollected"] = self.now.strftime("%b%d%Y %I%p")
            l.append(d)
            return l

        elif timeStep =="tenday": 

            today = datetime.date.today()
            dateList = []
            for i in range (1, 12):
                dateList.append((today + datetime.timedelta(days = i)).strftime("%b%d%Y"))
            l=[]    
            d = {}
            d["Description"] = soup.find_all("span",{"class":"DetailsSummary--extendedData--aaFeV"})
            d["MaxTempF"]=soup.find_all("span",{"class":"DailyContent--temp--_8DL5"})
            d["MinTempF"]=soup.find_all("span",{"class":"DailyContent--temp--_8DL5"})
            d["PrecipitationProb"]=soup.find_all("span",{"class":"DailyContent--value--3Xvjn","data-testid":"PercentageValue"})
            
            for i in range(1, 22,2):
                item = {}
                item["Date"] = dateList[int((i-1)/2)]
                item["Description"] = d["Description"][int(i/2)].text
                item["MaxTempF"] = int(d["MaxTempF"][i].text[:-1])
                item["MinTempF"] = int(d["MinTempF"][i+1].text[:-1])
                item["MaxTempC"] = self.convert.FartoCel(item["MaxTempF"])
                item["MinTempC"] = self.convert.FartoCel(item["MinTempF"])
                item["MaxTempK"] = self.convert.FartoKel(item["MaxTempF"])
                item["MinTempK"] = self.convert.FartoKel(item["MinTempF"])
                item["PrecipitationProb"]=int(d["PrecipitationProb"][i].text[:-1])
                item["TimeCollected"] = self.now.strftime("%b%d%Y %I%p")
                l.append(item)
            return l
        
        elif timeStep=="hourbyhour":
            hours = soup.find_all("h2",{"class":"DetailsSummary--daypartName--1Mebr", "data-testid":"daypartName"})
            temps = soup.find_all("span", {"class":"DetailsSummary--tempValue--RcZzi", "data-testid":"TemperatureValue"})
            PrecipitationProb = soup.find_all("div",{"class":"DetailsSummary--precip--2ARnx","data-testid":"Precip"})
            Description = soup.find_all("span",{"class":"DetailsSummary--extendedData--aaFeV"})

            today = datetime.date.today()
            days = []
            for i in range (0,3):
                days.append((today + datetime.timedelta(days = i)).strftime("%b%d%Y"))

            l = []
            count = 0
            for i in range (0,24):
                if (hours[i].text == '12 am'):
                    count +=1
                d={}
                d["Date"] = days[count]
                hourSplit = hours[i].text.split()
                d["Hour"] = hourSplit[0]+hourSplit[1].upper()
                d["PrecipitationProb"] = int(PrecipitationProb[i].find_all("span",{"data-testid":"PercentageValue"})[0].text[:-1])
                d["TemperatureF"] = int(temps[i].text[:-1])
                d["TemperatureC"] = self.convert.FartoCel(d["TemperatureF"])
                d["TemperatureK"] = self.convert.FartoKel(d["TemperatureF"])
                d["Description"] = Description[i].text
                d["TimeCollected"] = self.now.strftime("%b%d%Y %I%p")
                l.append(d)
            return l
        else:
            print("That is all options we have")

    def WCMongoWrite_tenday(self, URL, collectionname):
        """
        
        WCMongoWrite_tenday is a function which is used to write the inforamtion of the global list which is tenday_list 
        to the mongodb.
        - client object is creating a connection to the mongodb. 
        - After that the varialbe db is accessing Database object by creating a new one or accessing an exciting database. 
        - Finally, tenday_collection objec is accessing the Collection that is used to store the data in by 
          using insert_one() function.  
        
        """
        
        client = pymongo.MongoClient(URL)
        db = client[collectionname]
        collection_tenday = collectionname + "_tenday"
        tenday_collection = db[collection_tenday]
        for i in self.tenday_list:
            tenday_collection.insert_one(i)

    def WCMongoWrite_hourly(self, URL, collectionname):
        """
        
        WCMongoWrite_hourly is a function which is used to write the inforamtion of the global list which is hourly_list 
        to the mongodb.
        - client object is creating a connection to the mongodb. 
        - After that the varialbe db is accessing Database object by creating a new one or accessing an exciting database. 
        - Finally, hourly_collection objec is accessing the Collection that is used to store the data in by 
          using insert_one() function.  
        
        """
        client = pymongo.MongoClient(URL)
        db = client[collectionname]
        collection_hourly = collectionname + "_hourly"
        hourly_collection = db[collection_hourly]
        for i in self.hourly_list:
            hourly_collection.insert_one(i)
            
    def WCMongoWrite_today(self, URL, collectionname):
        """
        
        WCMongoWrite_today is a function which is used to write the inforamtion of the global list which is today_list 
        to the mongodb.
        - client object is creating a connection to the mongodb. 
        - After that the varialbe db is accessing Database object by creating a new one or accessing an exciting database. 
        - Finally, today_collection object is accessing the Collection that is used to store the data in by 
          using insert_one() function.  
        
        """
        client = pymongo.MongoClient(URL)
        db = client[collectionname]
        collection_today = collectionname + "_today"
        today_collection = db[collection_today]
        for i in self.today_list:
            today_collection.insert_one(i)


#w = WeatherCom()
