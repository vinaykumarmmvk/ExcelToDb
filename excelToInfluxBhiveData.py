import pandas as pd

from influxdb import InfluxDBClient

#1client = InfluxDBClient(host='se-psi40.netlab.hof-university.de', port=8086, username='vkmysuru', password='Mnvkm!23')
#client.create_database('vk_honeyfabric')
#1client.switch_database('vk_honeyfabric')

#Measurement Name
MEASUREMENT = 'beehiveData'

#Sheet Names
DAILY = 'daily'
HOUR = 'hourly'
FIVEMINUTE = '5-minutely'
WEEK = 'weekly'
MONTH = 'monthly'
YEAR = 'yearly'

SHEETS = [DAILY, HOUR, FIVEMINUTE, WEEK, MONTH, YEAR]

#Column Header Names
TIMECOL = 'time'
COL1 = 'weight (ini)'
COL2 = 'yield (sum)'
COL3 = 'temperature (min)'	
COL4 = 'temperature (avg)'	
COL5 = 'temperature (max)'	
COL6 = 'brood_temperature (min)'	
COL7 = 'brood_temperature (avg)'	
COL8 = 'brood_temperature (max)'
COL9 = 'humidity (min)'	
COL10 = 'humidity (avg)'	
COL11 = 'humidity (max)'	
COL12 = 'rain (sum)'	
COL13 = 'wind_speed (avg)'	
COL14 = 'wind_speed (max)'	
COL15 = 'wind_direction (ini)'	
COL16 = 'location'	
COL17 = 'location (comment)'	
COL18 = 'flow'	
COL19 = 'flow (comment)'	
COL20 = 'comment (title)'	
COL21 = 'comment (description)'

COLS = [TIMECOL,COL1,COL2,COL3,COL4,COL5,COL6,COL7,COL8,COL9,COL10,
COL11,COL12,COL13,COL14,COL15,COL16,COL17,COL18,COL19,COL20,COL21]

def fetchColValues(var1, var2):
    return pd.DataFrame(var1, columns=[var2]).to_numpy()
    
def colToJSONInsert(generalSheetName, sheetContent):
    TimeArr = []
    COL1Arr = []
    COL2Arr = []
    COL3Arr = []
    COL4Arr = []
    COL5Arr = []
    COL6Arr = []
    COL7Arr = []
    COL8Arr = []
    COL9Arr = []
    COL10Arr = []
    COL11Arr = []
    COL12Arr = []
    COL13Arr = []
    COL14Arr = []
    COL15Arr = []
    COL16Arr = []
    COL17Arr = []
    COL18Arr = []
    COL19Arr = []
    COL20Arr = []
    COL21Arr = []
    TimeArr = fetchColValues(sheetContent, TIMECOL)
    COL1Arr = fetchColValues(sheetContent, COL1)
    COL2Arr = fetchColValues(sheetContent, COL2)
    COL3Arr = fetchColValues(sheetContent, COL3)
    COL4Arr = fetchColValues(sheetContent, COL4)
    COL5Arr = fetchColValues(sheetContent, COL5)
    COL6Arr = fetchColValues(sheetContent, COL6)
    COL7Arr = fetchColValues(sheetContent, COL7)
    COL8Arr = fetchColValues(sheetContent, COL8)
    COL9Arr = fetchColValues(sheetContent, COL9)
    COL10Arr = fetchColValues(sheetContent, COL10)
    COL11Arr = fetchColValues(sheetContent, COL11)
    COL12Arr = fetchColValues(sheetContent, COL12)
    COL13Arr = fetchColValues(sheetContent, COL13)
    COL14Arr = fetchColValues(sheetContent, COL14)
    COL15Arr = fetchColValues(sheetContent, COL15)
    COL16Arr = fetchColValues(sheetContent, COL16)
    COL17Arr = fetchColValues(sheetContent, COL17)
    COL18Arr = fetchColValues(sheetContent, COL18)
    COL19Arr = fetchColValues(sheetContent, COL19)
    COL20Arr = fetchColValues(sheetContent, COL20)
    COL21Arr = fetchColValues(sheetContent, COL21)
    
    ArrLength = len(TimeArr)
    
    incr = 0
    
    while incr < ArrLength:
        json_body = [
            {
                "measurement": MEASUREMENT,
                "tags": {
                    "duration": generalSheetName
                },
                
                "fields": {
                TIMECOL : TimeArr[incr],
                COL1 : COL1Arr[incr] ,
                COL2 : COL2Arr[incr],
                COL3 : COL3Arr[incr],
                COL4 : COL4Arr[incr] ,
                COL5 : COL5Arr[incr],
                COL6 : COL6Arr[incr],
                COL7 : COL7Arr[incr] ,
                COL8 : COL8Arr[incr],
                COL9 : COL9Arr[incr],
                COL10 : COL10Arr[incr] ,
                COL11 : COL11Arr[incr] ,
                COL12 : COL12Arr[incr],
                COL13 : COL13Arr[incr],
                COL14 : COL14Arr[incr] ,
                COL15 : COL15Arr[incr],
                COL16 : str(COL16Arr[incr]),
                COL17 : str(COL17Arr[incr]) ,
                COL18 : str(COL18Arr[incr]),
                COL19 : str(COL19Arr[incr]),
                COL20 : str(COL20Arr[incr]) ,
                COL21 : str(COL21Arr[incr])
                }
            }
        ]

        #1print(client.write_points(json_body))
        print(json_body)
        incr += 1
        
    print("success")
    
def addDataToInflux(temp):
    
    dataSheet1 = pd.read_excel (r'D:/BeehiveData/2019-Island-01-Beehive-01.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet1)
    dataSheet2 = pd.read_excel (r'D:/BeehiveData/2019-Island-01-Beehive-02.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet2)
    dataSheet3 = pd.read_excel (r'D:/BeehiveData/2019-Island-02-Beehive-01.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet3)
    dataSheet4 = pd.read_excel (r'D:/BeehiveData/2019-Island-02-Beehive-02.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet4)
    dataSheet5 = pd.read_excel (r'D:/BeehiveData/2019-Island-02-Beehive-03.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet5)
    dataSheet6 = pd.read_excel (r'D:/BeehiveData/2019-Island-02-Beehive-04.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet6)
    dataSheet7 = pd.read_excel (r'D:/BeehiveData/2020-Island-01-Beehive-01.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet7)
    dataSheet8 = pd.read_excel (r'D:/BeehiveData/2020-Island-01-Beehive-02.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet8)
    dataSheet9 = pd.read_excel (r'D:/BeehiveData/2020-Island-02-Beehive-01.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet9)
    dataSheet10 = pd.read_excel (r'D:/BeehiveData/2020-Island-02-Beehive-02.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet10)
    dataSheet11 = pd.read_excel (r'D:/BeehiveData/2020-Island-02-Beehive-03.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet11)
    dataSheet12 = pd.read_excel (r'D:/BeehiveData/2020-Island-02-Beehive-04.xlsx', sheet_name=temp)
    colToJSONInsert(temp, dataSheet12)
       
    
for x in SHEETS:
    addDataToInflux(x)
    print("x in sheets for loop")
    




