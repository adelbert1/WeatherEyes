
class Convert:
    def __init__(self):
        """Since this is a utility class, there is not a need for global variables.

        Import required to call class from another Python file:
        from folder.file import Convert

        Example of calling:
        Temp = Convert()
        Ftemp = Temp.CeltoFar(100) """
        pass

    def FartoKel(self,tempF):
        """This method converts a temperature from Farenheit to Kelvin.
        args:
        tempF: integer that represents temperature in Farenheit

        returns:
        tempK: integer that represents temperature in Kelvin"""
        tempK = round(((tempF - 32)*(5/9))+273.15)
        return tempK

    def FartoCel (self,tempF):
        """This method converts a temperature from Farenheit to Celsius.
        args:
        tempF: integer that represents temperature in Farenheit

        returns:
        tempC: integer that represents temperature inCCelsius"""
        
        tempC = round((tempF - 32)*(5/9))
        return tempC

    def CeltoFar (self,tempC):
        """This method converts a temperature from Celsius to Farenheit.
        args:
        tempC: integer that represents temperature in Celsius

        returns:
        tempF: integer that represents temperature in Farenheit"""
        
        tempF = round((tempC *(9/5))+32)
        return tempF

    def InchtoMM (self,inch):
        """This method converts an accumulation amount from inches to millimeters.
        args:
        inch: integer that represents accumulation amount in inches

        returns:
        MM: integer that represents accumulation amount in millimeters"""
        MM = round((inch * 24.5),1)
        return MM

