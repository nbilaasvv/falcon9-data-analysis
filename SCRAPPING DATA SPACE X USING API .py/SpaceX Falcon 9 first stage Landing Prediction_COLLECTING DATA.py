#Import Lubraries and define auxiliary functions
import requests
import pandas as pd
import numpy as np
import datetime

#build dataframe (When displaying a DataFrame, show every column and show all text in each cell, without truncation)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

#using API to extract information using identification number in the launch data

##from rocket column extract booster name
def getBoosterVersion(data):
    for x in data['rocket']:
        if x:
            response = requests.get("https://api.spacexdata.com/v4/rockets/"+str(x)).json()
            BoosterVersion.append(response['name'])

##extract launch site info (longitude, latitude) from launch pad
def getLaunchSite(data):
    for x in data['launchpad']:
        if x:
            response = requests.get("https://api.spacexdata.com/v4/launchpads/"+str(x)).json()
            Longitude.append(response['longitude'])
            Latitude.append(response['latitude'])
            LaunchSite.append(response['name'])

##extract mass of payload and orbit going to from payload
def getPayloadData(data):
    for load in data['payloads']:
        if load:
            response = requests.get("https://api.spacexdata.com/v4/payloads/"+load).json()
            PayloadMass.append(response['mass_kg'])
            Orbit.append(response['orbit'])

##extract outcomes of landing, types of landings, number of flights with the core, gridfins used, core used, number of time specifi core has been used from cores
def getCoreData(data):
    for core in data['cores']:
        if core['core'] !=None:
            response = requests.get("https://api.spacexdata.com/v4/cores/"+core['core']).json()
            Block.append(response['block'])
            ReusedCount.append(response['reuse_count'])
            Serial.append(response['serial'])
        else:
            Block.append(None)
            ReusedCount.append(None)
            Serial.append(None)
        Outcome.append(str(core['landing_success'])+' '+str(core['landing_type']))
        Flights.append(core['flight'])
        GridFins.append(core['gridfins'])
        Reused.append(core['reused'])
        Legs.append(core['legs'])
        LandingPad.append(core['landpad'])

#request rocket launch data from SpaceX API 
spacex_url="https://api.spacexdata.com/v4/launches/past"
response = requests.get(spacex_url)

#Send a GET request to SpaceX’s API to retrieve the launch data, then convert (parse) that data into program used

static_json_url='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/datasets/API_call_spacex_api.json'

response = requests.get(static_json_url)

# Use json_normalize meethod to convert the json result into a dataframe
##Convert response to Python list/dict
data = response.json()

##Flatten into a DataFrame
data = pd.json_normalize(data)

# Get the head of the dataframe
print(data.head())

#Use API to get info about the launches using the IDs goven for each launch 

##take a subset of dataframe keeping only the features wanted and flight number, and date utcd
data = data[['rocket', 'payloads', 'launchpad', 'cores', 'flight_number', 'date_utc']]

##remove rows with multiple cores to make sure there are only one cores and one payloads
data = data[data['cores'].map(len)==1]
data = data[data['payloads'].map(len)==1]

##after make cores and payloads lists of size 1 then make it single value and remove the feature
data['cores'] = data['cores'].map(lambda x : x[0])
data['payloads'] = data['payloads'].map(lambda x : x[0])

##convert date_utc to datetime datatype then extract the date leaving the time
data['date'] = pd.to_datetime(data['date_utc']).dt.date

#restrict the dates of the launches using date 
data = data[data['date']<= datetime.date(2020, 11, 13)]

#Global variables
BoosterVersion = []
PayloadMass = []
Orbit = []
LaunchSite = []
Outcome = []
Flights = []
GridFins = []
Reused = []
Legs = []
LandingPad = []
Block = []
ReusedCount = []
Serial = []
Longitude = []
Latitude = []

# Call getBoosterVersion
getBoosterVersion(data)

BoosterVersion[0:5]

# Call getLaunchSite
getLaunchSite(data)

# Call getPayloadData
getPayloadData(data)

# Call getCoreData
getCoreData(data)

#construct dataset combine all data into a dictionary
launch_dict = {'FlightNumber': list(data['flight_number']), 
               'Date': list(data['date']),
               'BoosterVersion': BoosterVersion,
               'PayloadMass': PayloadMass,
               'Orbit': Orbit,
               'LaunchSite': LaunchSite,
               'Outcome': Outcome,
               'Flights': Flights,
               'GridFins': GridFins,
               'Reused': Reused,
               'Legs': Legs,
               'LandingPad': LandingPad,
               'Block': Block,
               'ReusedCount': ReusedCount,
               'Serial': Serial,
               'Longitude': Longitude,
               'Latitude': Latitude}

# #check lenght each value in launch dict
for key, value in launch_dict.items():
    print(f"{key}: {len(value)}")

# #cut file that not have the same length 
min_len = min(len(v) for v in launch_dict.values())
for k in launch_dict:
    print(k)
    launch_dict[k] = launch_dict[k][:min_len]

# #create a data from launch dict
launch_df = pd.DataFrame(launch_dict)

# #Removes / filter falcon 1 so it only include falcon 9 launches
data_falcon9 = launch_df[launch_df['BoosterVersion'] != 'Falcon 1']

#reset flight number columns
data_falcon9 = data_falcon9.copy()

# Reset flight number sequentially
data_falcon9['flight_number'] = range(1, len(data_falcon9) + 1)

#DATA WRANGLING
data_falcon9.isnull().sum()

# Calculate the mean of PayloadMass (ignoring NaN)
data_falcon9 = data_falcon9.copy()
payload_mean = data_falcon9['PayloadMass'].mean()

# Replace np.nan values with the mean
data_falcon9['PayloadMass'] = data_falcon9['PayloadMass'].fillna(payload_mean)

data_falcon9.to_csv(r'C:\Users\ACER\Downloads\dataset_part_1.csv', index=False)
