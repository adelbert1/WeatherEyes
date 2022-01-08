import pymongo
from pymongo import MongoClient
import pprint
import datetime
from datetime import date
from .preferences import Preferences

class WebpageMongo:

    def __init__(self,user):
      self.dates = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dev']
      self.client = pymongo.MongoClient("mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
      self.user = user
      self.Preferences = Preferences(self.user)
      self.dbhist = self.client["historic"]
      self.collectiondayhist = self.dbhist["historic_dailyobs"]
      self.collectionhourhist = self.dbhist["historic_hourlyobs"]
      
    def Single_Day_Inquriy_Obs(self, date):
      date = date.split()
      new_date = date[0]+date[1]+date[2]
      results = self.collectiondayhist.find({"Date": new_date})
      High = self.Preferences.HighTemp()
      Low = self.Preferences.LowTemp()
      Rainfall = self.Preferences.Rainunit()
      Snowfall = self.Preferences.Snowunit()
      TempUnit = self.Preferences.UnitT()
      PreUnit = self.Preferences.UnitP()
      for x in results:
          High = str(x[High]) + TempUnit
          Low = str(x[Low]) + TempUnit
          if str(x[Rainfall]) == "Trace amount":
            Rain = "Trace amount"
          if str(x[Rainfall]) != "Trace amount":
            Rain = str(x[Rainfall]) + " " + PreUnit
          if str(x[Snowfall]) == "Trace amount":
            Snow = "Trace amount"
          if str(x[Snowfall]) != "Trace amount":
            Snow = str(x[Snowfall]) + " " + PreUnit
      WDate = date[0] + " " + date[1] + " " + date[2]
      Webreturn = {"Date" : WDate , "High" : High , "Low" : Low , "Rainfall" : Rain , "Snowfall" : Snow}
      return Webreturn

    def Single_Day_Inquiry_Hourly_Obs(self,date):
      date = date.split()
      Date = date[0] + date[1] + date[2]
      time = self.time_convert(date[3], date[4])
      Precipitation = self.Preferences.Precipunit()
      Temp = self.Preferences.Temp()
      results = self.collectionhourhist.find({"Date":Date, "Time": time})
      TempUnit = self.Preferences.UnitT()
      PreUnit = self.Preferences.UnitP()
      for x in results:
        Temp = str(x[Temp]) + TempUnit
        Precip = str(x[Precipitation])
        if str(x[Precipitation]) == "No precipitation":
          Precip = "No precipitation"
        if str(x[Precipitation]) == "Trace amount":
          Precip = "Trace amount"
        if str(x[Precipitation]) != "No precipitation" and str(x[Precipitation]) != "Trace amount":
          Precip =  str(x[Precipitation]) + " " +  PreUnit
      time_per =  date[0] + " " + date[1] + " " + date[2] + " " + time
      Webreturn = {"Date" : time_per , "Temp" : Temp , "Precipitation" : Precip}
      return Webreturn

    def Interval_Inquiry_Hourly_Obs(self, date):
      date = date.split()
      start_date = date[0] + date[1] + date[2]
      sd = date[0] + " " + date[1] + " " + date[2]
      start_time = self.time_convert(date[3], date[4])
      end_date = date[6] + date[7] + date[8]
      ed = date[6] + " " + date[7] + " " + date[8]
      end_time = self.time_convert(date[9], date[10])
      temps = []
      rain = []
      delta = datetime.timedelta(days = 1)
      start = datetime.date(int(date[2]), self.recodedate_num(start_date[:3]), int(date[1]))
      end = datetime.date(int(date[8]), self.recodedate_num(end_date[:3]), int(date[7]))
      Precipitation = self.Preferences.Precipunit()
      Temp = self.Preferences.Temp()
      TempUnit = self.Preferences.UnitT()
      PreUnit = self.Preferences.UnitP()
      while start <= end:
          year = start.strftime('%Y')
          month = self.recodedate_str(start.strftime('%m'))
          day = start.strftime('%d')
          search_date = month + day + year
          switch = 0 
          while switch == 0:
              results = self.collectionhourhist.find({"Date":search_date, "Time": start_time})
              for x in results:
                  temps.append(float(x[Temp]))
                  if type(x[Precipitation]) == float or x[Precipitation].isdigit():
                      rain.append(float(x[Precipitation]))
              start_time = self.iterate_time(start_time)
              if start_time == "1:00AM":
                  switch = 1
          switch = 0
          start = start + delta
      start = sd + " " + start_time
      end = ed + " " + end_time
      high = str(max(temps)) + " " + TempUnit
      low = str(min(temps)) + " " + TempUnit
      Totalrain = str(sum(rain)) + " " +PreUnit
      Webreturn = {"Start" : start , "End" : end , "MaxTemp" : high , "MinTemp" : low , "Rainfall" : Totalrain}
      return Webreturn

    def Interval_Inquiry(self, start_date, end_date):
      start_date = start_date.split()
      end_date = end_date.split()
      start = date(int(start_date[2]), self.recodedate_num(start_date[0]), int(start_date[1]))
      end = date(int(end_date[2]), self.recodedate_num(end_date[0]), int(end_date[1]))
      delta = datetime.timedelta(days = 1)
      temps = []
      snow = []
      rain = []
      High = self.Preferences.HighTemp()
      Low = self.Preferences.LowTemp()
      Rainfall = self.Preferences.Rainunit()
      Snowfall = self.Preferences.Snowunit()
      TempUnit = self.Preferences.UnitT()
      PreUnit = self.Preferences.UnitP()
      while start <= end:
          year = start.strftime('%Y')
          month = self.recodedate_str(start.strftime('%m'))
          day = start.strftime('%d')
          new_date = month+day+year
          results = self.collectiondayhist.find({"Date": new_date})
          for x in results:
              temps.append(float(x[High]))
              temps.append(float(x[Low]))
              if type(x[Rainfall]) == float or x[Rainfall].isdigit():
                  rain.append(float(x[Rainfall]))
              if type(x[Snowfall]) == float or x[Snowfall].isdigit():
                  snow.append(float(x[Snowfall]))
          start = start + delta
      high = str(max(temps)) + " "+ TempUnit
      low = str(min(temps)) + " " +TempUnit
      Totalsnow = str(sum(snow)) + " " + PreUnit
      Totalrain = str(sum(rain)) + " " + PreUnit
      start = start_date[0] + " " + start_date[1] + " " + start_date[2]
      end = end_date[0] + " " + end_date[1] + " " + end_date[2]
      Webreturn = {"Start" : start , "End" : end , "MaxTemp" : high , "MinTemp" : low , "Snowfall" : Totalsnow , "Rainfall" : Totalrain}
      return Webreturn
                    
             
    def Current_Forecast(self):
      daten = datetime.datetime.now() -datetime.timedelta(hours =4)
      datet = daten.strftime("%b %d %Y")
      timet = daten.strftime("%I")
      if int(timet[0]) == 0:
        timet = daten.strftime("%I")[1]
      am_pm = daten.strftime("%p")
      ah = self.Forecast_Inquiry_M2(datet, timet, am_pm, "AccuWeather", "Hourly")
      ad = self.Forecast_Inquiry_M2(datet, timet, am_pm, "AccuWeather", "Daily")
      wh = self.Forecast_Inquiry_M2(datet, timet, am_pm, "WeatherCom", "Hourly")
      wd = self.Forecast_Inquiry_M2(datet, timet, am_pm, "WeatherCom", "Daily")
      nh = self.Forecast_Inquiry_M2(datet, timet, am_pm, "NatWeather", "Hourly")
      nd = self.Forecast_Inquiry_M2(datet, timet, am_pm, "NatWeather", "Daily")
      return [ah, ad, nh, nd, wh, wd]


    def recodedate_num(self, date):
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
        time = str(time) + ":00"
        if ampm == 'AM':
            time = str(time) + "AM"
        if ampm == 'PM':
            time = str(time) + "PM"
        return time

    def iterate_time(self, time):
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

    def detinqtype(self, HRorDAY, range_choice, start_date, end_date, start_time, end_time, start_am_pm, end_am_pm):
      start_time = start_time + " " + start_am_pm
      end_time = end_time + " " + end_am_pm
      start_date = start_date.strftime('%b %d %Y')
      end_date = end_date.strftime('%b %d %Y')
      if HRorDAY == "Daily":
          if range_choice == "Range of days":
              data = self.Interval_Inquiry(start_date, end_date)
          if range_choice == "Single day":
              data = self.Single_Day_Inquriy_Obs(start_date)
      if HRorDAY == "Hourly":
          if range_choice == "Range of days":
              interval = start_date + " "+ start_time + " - " + end_date + " " + end_time
              data = self.Interval_Inquiry_Hourly_Obs(interval)
          if range_choice == "Single day":
              hourdate = start_date + " " + start_time
              data = self.Single_Day_Inquiry_Hourly_Obs(hourdate)
      return data

    def Forecast_Inquiry_M2(self, date, time, am_pm,  service_prov, timeinterval):
        if len(time) == 1:
            if service_prov == "AccuWeather" or service_prov == "WeatherCom":
                time = "0" + time
        search_time = time + am_pm
        date = date.split()
        date = date[0] + date[1] + date[2]
        collection = self.detdatabase(service_prov, timeinterval)
        search_date = date +  " " + search_time
        results = collection.find({"TimeCollected" : search_date})
        MaxTemp = self.Preferences.MaxTemp() #tenday
        MinTemp = self.Preferences.MinTemp() #tenday
        Temp = self.Preferences.Temp() #hourly
        TempUnit = self.Preferences.UnitT()
        Listionary = []
        if timeinterval == "Daily":
            if service_prov == "NatWeather":
                counter = 0
                for x in results:
                    Date = x["Date"]
                    Min = str(x[MinTemp]) + TempUnit
                    Max = str(x[MaxTemp]) + TempUnit
                    Date = Date[0:3] + " " + Date[3:5] + " " + Date[5:]
                    Docs = {"Date" : Date ,  "Precipitation_Probability" : x["PrecipitationProb"],
                            "Description" : x["Description"] , "MinTemp" : Min  , "MaxTemp": Max}
                    Listionary.append(Docs)
                    counter = counter + 1
                    if counter == 7:
                        break
                return Listionary
            if service_prov == "AccuWeather" or service_prov == "WeatherCom":
                counter = 0
                for x in results:
                    Date = x["Date"]
                    Min = str(x[MinTemp]) +" " + TempUnit
                    Max = str(x[MaxTemp]) + " " +TempUnit
                    Date = Date[0:3] + " " + Date[3:5] + " " + Date[5:]
                    Docs = {"Date" : Date ,  "Precipitation_Probability" : x["PrecipitationProb"],
                            "Description" : x["Description"] , "MinTemp" : Min  , "MaxTemp": Max}
                    Listionary.append(Docs)
                    counter = counter + 1
                    if counter == 10:
                        break
                return Listionary
        if timeinterval == "Hourly":
            if service_prov == "NatWeather":
                counter = 0
                for x in results:
                    Date = x["Date"]
                    Temperature = str(x[Temp]) + " " + TempUnit
                    Date = Date[0:3] + " " + Date[3:5] + " " + Date[5:]
                    Docs = {"Date": Date, "Description" : x["Description"], "Hour" : x["Hour"],
                            "Temperature": Temperature}
                    Listionary.append(Docs)
                    counter = counter + 1
                    if counter == 25:
                        break
                return Listionary
            if service_prov == "WeatherCom" or service_prov == "AccuWeather":
                counter = 0
                for x in results:
                    Date = x["Date"]
                    Temperature = str(x[Temp]) +" "+ TempUnit
                    Date = Date[0:3] + " " + Date[3:5] + " " + Date[5:]
                    Docs = {"Date": Date, "Description" : x["Description"], "Hour" : x["Hour"],
                            "Temperature": Temperature, "Precipitation_Probability" : x["PrecipitationProb"]}
                    Listionary.append(Docs)
                    counter = counter + 1
                    if counter == 24:
                        break
                return Listionary
  
    def detdatabase(self, service_prov, timeinterval):
      db = self.client[service_prov]
      if timeinterval == "Daily":
          if service_prov == "NatWeather":
              timeinterval = "sevenday"
          if service_prov == "AccuWeather" or service_prov == "WeatherCom":
              timeinterval = "tenday"
      if timeinterval == "Hourly":
          timeinterval = "hourly"
      collect_name = service_prov + "_" + timeinterval
      collection = db[collect_name]
      return collection
