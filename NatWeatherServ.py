import json
import urllib.request
import datetime
import pymongo
from pymongo import MongoClient
from convert import Convert

class NatWeatherServ:
    '''This is a class that scrapes, formats, and writes forecast information from National Weather Service API to MongoDB. 
    The forecast information is a hourly forecast for next 24 hours and daily forecast for next seven days from point of collection.'''
    def __init__(self):
        '''This is the constructor for the class. There are several global variables: 
        \n sevendayminmaxprecip - list of dictionaries, each including the date of forecast, time collected, minimum temperature, maximum temperature, and precipitation probability for the next seven days. 
        \n sevendaydescr - list of dictionaries, each including the date of forecast, time collected, and short description of weather
        \n sevendayforecast - list of sevendayminmaxprecip and sevendaydescr merged to create individual dictionaries per day with all information
        \n timecollected - the time at which the class is called - the time is formatted using methods below'''
        self.sevendayminmaxprecip = []
        self.sevendaydescr = []
        self.sevendayforecast = []
        self.hourlyforecast = []
        timerightnow = str(datetime.datetime.now())
        date = self.convertdate(timerightnow)
        time = self.converttime(timerightnow)
        self.timecollected = date + " " + time


    def JsonFile (self, link):
        ''' JsonFile method is for converting a link to an API, passed as a parameter, into a JSON file. The method returns the JSON file.'''
        forecastapi = link
        apiurl = urllib.request.urlopen(forecastapi)
        req = apiurl.read()
        forecast_json = json.loads(req.decode())
        return forecast_json
        
    def convertdate (self, date):
        '''Convertdate method is for converting the date, passed as a parameter, into a standard and more user-friendly format. The method returns the date as a string.'''
        date = date[:10]
        months = {"01":"Jan", "02":"Feb", "03":"Mar", "04":"Apr", "05":"May", "06":"June","07":"July", "08":"Aug", "09":"Sept", "10":"Oct", "11":"Nov", "12":"Dec"} 
        year = date[:4]
        month = date [5:7]
        day = date[8:]
        monthabr = months.get(month)
        date = monthabr + day + year
        return date

    def converttime(self,time):
        '''Converttime method is for converting time, passed as a parameter, into a standard and more user-friendly format. The method returns the time as a string.'''
        hour = time[11:13]
        numhour = int(hour)
        if numhour - 12 < 0:
            amorpm = 'AM'
        else:
            amorpm = 'PM'
        modhour = numhour%12
        if modhour == 0:
            modhour = 12
        else:
            modhour = modhour
        strhour = str(modhour)
        formattime = strhour + amorpm
        return formattime
        
    def convertkeysandvals (self,dictlist,newkey1, newkey2):
        ''' This method is for converting keys and values from the API that provided minimum temperature, maximum temperature, and
        precipitation probability into a standardized and more user-friendly appearance. The parameters passed to convertkeysandvals are the dictionary
        list that needs conversion as well as the new keys that each dictionary will be converted to. Each dictionary list has elements that are dictionaries with only
        two keys each: date and forecast data topic. The values of these keys are formatted accordingly within the method as well. The parameter passed, dictlist, is updated within
        the method and then returned.'''
        firstdict = dictlist[0]
        keys = [*firstdict.keys()] 
        key1 = keys[0]
        key2 = keys[1]
        for dictionary in dictlist:
            dictionary[newkey1] = dictionary.pop(key1)
            dictionary[newkey2] = dictionary.pop(key2)
            date = dictionary[newkey1]
            date = self.convertdate(date)
            dictionary[newkey1] = date
            
            if newkey2 == 'MinTempC':
                lowtemp = dictionary[newkey2]
                dictionary[newkey2]= round(lowtemp)

            elif newkey2 == 'MaxTempC':
                hightemp = dictionary[newkey2]
                dictionary[newkey2]= round(hightemp)

            elif newkey2 == 'PrecipitationProb':
                percentprecip = dictionary[newkey2]
                dictionary[newkey2] = percentprecip
                
        return (dictlist)
        
    def hourly (self, URL, collectionname):
        '''Hourly method is for scraping, formatting, and writing to MongoDB the hourly forecast data. The parameters passed are the URL and 
        collection name used to successfully write into MongoDB.'''
        hourlydict = self.JsonFile('https://api.weather.gov/gridpoints/PBZ/77,65/forecast/hourly')
        propertiesdict = hourlydict.get('properties')
        hourlyforecast = propertiesdict.get('periods')
        hourly24 = []
        i=0
        Temp = Convert()
        while i < 24:
            hourly24.append(hourlyforecast[i])
            i+=1
        for dictionary in hourly24:
            dictionary.pop('number')
            dictionary.pop('name')
            dictionary.pop('isDaytime')
            dictionary.pop('temperatureUnit')
            dictionary.pop('temperatureTrend')
            dictionary.pop('windSpeed')
            dictionary.pop('windDirection')
            dictionary.pop('icon')
            dictionary.pop('detailedForecast')
            dictionary.pop('endTime')
            startDateandTime = dictionary['startTime']
            startDate = self.convertdate(startDateandTime)
            startTime = self.converttime(startDateandTime)
            dictionary['Date'] = startDate
            dictionary['startTime'] = startTime
            dictionary['Hour'] = dictionary.pop('startTime')
            dictionary['Description'] =dictionary.pop('shortForecast')
            dictionary['TimeCollected'] = self.timecollected
            dictionary ['TemperatureF'] = dictionary.pop('temperature')
            dictionary['TemperatureC'] = Temp.FartoCel(dictionary['TemperatureF'])
            dictionary ['TemperatureK'] = Temp.FartoKel(dictionary['TemperatureF'])
        self.hourlyforecast = hourly24
        self.mongohourly(URL, collectionname)
  
    def sevendayforecastfunc(self, URL, collectionname):
        '''This method is for initiating another method (sevendaysplit) for the collection of the minimum temperature, maximum temperature, and preciptation probability 
        for the next seven days, as well as initiating another method (descr) for the description of forecast for the next seven days. This method then merges the information 
        from two separate APIs into one list of dictionaries, sevendayforecast, which is then written to MongoDB. The parameters passed are the URL and collection name used to 
        successfully write into MongoDB.'''
        self.descr('https://api.weather.gov/gridpoints/PBZ/77,65/forecast')
        self.sevendaysplit('https://api.weather.gov/gridpoints/PBZ/77,65')
        i = 0
        for minmaxprecipdict in self.sevendayminmaxprecip:
            descrdict = self.sevendaydescr[i]
            sevendaydict = minmaxprecipdict|descrdict
            self.sevendayforecast.append(sevendaydict)
            i+=1
        self.mongodaily(URL, collectionname)
  
    
    def sevendaysplit (self, link):
        '''This method is for scraping the seven day forecast information for the minimum temperature, maximum temperature, and precipitation probability. The parameter 
        passed is the link to the API for the aforementioned information. After the information is scraped, the code will format the keys and values to be more user-friendly. 
        For precipitation probability, the code compiles the projected probabilities given from the API per day and creates an average probability for the day. 
        Each separate list of dictionaries per topic (minimum temperature, maximum temperature, and precipitation probability) is then merged into one comprehensive list.'''
        sevendaysplitdict = self.JsonFile(link)
        propertiesdict = sevendaysplitdict.get('properties')
        mintempdict = propertiesdict.get('minTemperature')
        mintemplist = mintempdict.get('values')
        mintempdict = list(self.convertkeysandvals(mintemplist[:7],'Date', 'MinTempC'))
        Temp = Convert()
        for dictionary in mintempdict:
            dictionary['MinTempF'] = Temp.CeltoFar(dictionary['MinTempC'])
            dictionary ['MinTempK'] = Temp.FartoKel(dictionary['MinTempF'])
        
        maxtempdict = propertiesdict.get('maxTemperature')
        maxtemplist = maxtempdict.get('values')
        maxtempdict = list(self.convertkeysandvals(maxtemplist[:7],'Date', 'MaxTempC'))
        for dictionary in maxtempdict:
            dictionary['MaxTempF'] = Temp.CeltoFar(dictionary['MaxTempC'])
            dictionary ['MaxTempK'] = Temp.FartoKel(dictionary['MaxTempF'])
        
        
        proprecipdict = propertiesdict.get('probabilityOfPrecipitation')
        propreciplist = proprecipdict.get('values')
        proprecipdict2 = list(self.convertkeysandvals(propreciplist,'Date', 'PrecipitationProb'))
        minmaxlist = []
        preciplist = []
        megalist = []
        newprecipdict = []
        precipkeys = []
        for x in range(len(proprecipdict2)):
            if x+1 ==  len(proprecipdict2):
                break
            elif proprecipdict2[x]['Date'] == proprecipdict2[x+1]['Date']:
                precipkeys.append(proprecipdict2[x]['PrecipitationProb'])
            else:
                precipkeys.append(proprecipdict2[x]['PrecipitationProb'])
                newprecipdict.append({'Date' : proprecipdict2[x]['Date'],'PrecipitationProb' : str(sum(precipkeys)/len(precipkeys))})
                precipkeys = []
        for precipdict in newprecipdict:
            precipdict['PrecipitationProb'] = round(float(precipdict['PrecipitationProb']))
        i = 0
        k = 0
        for mindict in mintempdict:
            maxdict = maxtempdict[i]
            minmaxmerge = mindict | maxdict
            minmaxlist.append(minmaxmerge)
            i = i + 1
        minmaxlist = minmaxlist[:7]
        for minmaxdict in minmaxlist:
            precipdict1 = newprecipdict[k]
            megamerge = minmaxdict | precipdict1
            megalist.append(megamerge)
            k +=1
        self.sevendayminmaxprecip = megalist
                                   
    def descr (self,link):
        '''This method is for scraping, formatting, and appending the sevendaydescr list for the forecast description for the next seven days. The parameter passed
        is the link to the API that has forecast description data.'''
        descrdaydict = self.JsonFile(link)
        propertiesdict = descrdaydict.get('properties')
        dayforecast = propertiesdict.get('periods')
        dailyforecast = []
        for dictionary in dayforecast:
            dictionary.pop('number')
            dictionary.pop('name')
            dictionary.pop('endTime')
            dictionary.pop('temperature')
            dictionary.pop('temperatureUnit')
            dictionary.pop('temperatureTrend')
            dictionary.pop('windSpeed')
            dictionary.pop('windDirection')
            dictionary.pop('icon')
            dictionary.pop('detailedForecast')
            if dictionary['isDaytime'] == True:
                dictionary.pop('isDaytime')
                dailyforecast.append(dictionary)
            else:
                dictionary.pop('isDaytime')
        for dictionary in dailyforecast:
            startDateandTime = dictionary['startTime']
            startDate = self.convertdate(startDateandTime)
            dictionary['startTime'] = startDate
            dictionary['Date'] = dictionary.pop('startTime')
            dictionary['Description'] = dictionary.pop('shortForecast')
            dictionary['TimeCollected'] = self.timecollected
            self.sevendaydescr.append(dictionary)

    def mongo(self, URL):
        '''This method is for connecting a client to MongoDB, the shared database. The parameter passed is the URL to the cluster in MongoDB. The client connection is returned.'''
        client = pymongo.MongoClient(URL)
        return (client)
    
    def mongohourly(self, URL, collectionname):
        '''This method is for writing the hourly forecast data into MongoDB. The parameters passed are the URL to the cluster in MongoDB and the collection name of the database.'''
        client = self.mongo(URL)
        db = client[collectionname]
        collection_hourly = collectionname + "_hourly"
        collection = db[collection_hourly]
        for dictionary in self.hourlyforecast:
            new_data = dictionary
            post_id = collection.insert_one(new_data).inserted_id
        
    def mongodaily(self, URL, collectionname):
        '''This method is for writing the daily forecast data into MongoDB. The parameters passed are the URL to the cluster in MongoDB and the collection name of the database.'''
        client = self.mongo(URL)
        db = client[collectionname]
        collection_daily = collectionname + "_sevenday"
        collection = db[collection_daily]
        collection.insert_many(self.sevendayforecast)

