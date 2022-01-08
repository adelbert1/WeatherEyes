from NatWeatherServ import NatWeatherServ
from AccuWeather_WebScraper import AccuWeather_forecast
from WeatherCom import WeatherCom
from HistoricHourlyScraper import HistoricHourlyScraper
from HistoricScraper import HistoricScraper
import time

class WeatherEyesMain:
    """
    This class is designed to allow a user to store weather forecast data in a mongodb database every hour.
    Additionally, it allows a user to inquire into Historic weather data provided by weather.gov
    """
    def __init__(self):
        self.URL = "mongodb+srv://team:team@cluster0.yknbr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
        self.counter = 0
    
    def Update_Data(self):
        """
        Calling this method will scrape data from the National Weather Service, AccuWeather, and Weather.com.
        This data will include a 10 day forecast from AccuWeather and Weather.com as well as a 7 day forecast from
        the National Weather Service. It will also pull a 24 forecast from each weather service. It will also scrape new
        observational data from weather.gov. After scraping this data, this method will write the data into Mongodb.
        Within Mongodb this data will be sorted by weather service provider and type of forecast (hourly vs 10/7 day)
        """
        while True:
            try:
                self.NatWeather = NatWeatherServ()
                self.NatWeather.sevendayforecastfunc(self.URL, "NatWeather")    
                self.NatWeather.hourly(self.URL, "NatWeather")
            except:
                try:
                    for x in range(5):
                        time.sleep(20)
                        self.NatWeather = NatWeatherServ()
                        self.NatWeather.sevendayforecastfunc(self.URL, "NatWeather")    
                        self.NatWeather.hourly(self.URL, "NatWeather")
                except:
                    print("The National Weather Service failed to connect, please have this method run independetly with Update_NWS to just update the NWS")
            finally:
                self.AccuWeather = AccuWeather_forecast()
                self.AccuWeather.hourly_forecast()
                self.AccuWeather.daily_forecast()
                self.AccuWeather.write_to_mongodb(self.URL, "AccuWeather")
                self.WeatherCom = WeatherCom()
                self.WeatherCom.WCMongoWrite_tenday(self.URL, "WeatherCom")
                self.WeatherCom.WCMongoWrite_hourly(self.URL, "WeatherCom")
                self.HistoricHourly = HistoricHourlyScraper()
                self.HistoricHourly.write_hourly_mongo(self.URL, "historic") 
                print("Data has been written to database")
                self.counter += 1
                if self.counter % 24 == 0:
                    self.HistoricScraper = HistoricScraper()
                    self.HistoricScraper.write_daily_mongo(self.URL, "historic")
                time.sleep(3600)

    def Update_NWS(self):
        """
        As the NWS web service occasinally results in a sporatic 500 error we have included a method that
        can update the data in the NWS service manually. This will only be necessary when prompted by the
        Update_Data method. Otherwise the Update_Data method will collect this data.
        """
        try:
            self.NatWeather = NatWeatherServ()
            self.NatWeather.sevendayforecastfunc(self.URL, "NatWeather")
            self.NatWeather.hourly(self.URL, "NatWeather")
        except:
            print("National Weather Service Produced an exception, please have this service's data run again")

    def Initialize_History(self):
        """
        Call this method to initialize a database of historical data from weather.gov, it will write all available
        observational data from weather.gov to mongodb
        """
        self.HistoricScraper = HistoricScraper()
        self.HistoricScraper.write_many_mongo(self.URL, "historical")
        
    
    
if __name__ == "__main__":
    Main = WeatherEyesMain()
    Main.Update_Data()
