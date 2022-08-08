import urllib.request, urllib.parse, urllib.error
import http
import sqlite3
import json
import time
import ssl
import sys
import pandas as pd

#time imports

from datetime import timezone
from datetime import datetime

from datetime import date
from datetime import timedelta
from datetime import datetime

#others
import requests
from pprint import pprint
import os


#weather API
api_key='6e23fd2fe2978304d943c2ec1a99358f'

#connecting with sqlite3
conn = sqlite3.connect('weatherdata.sqlite')

cur = conn.cursor()

#creating table
cur.execute('''CREATE TABLE IF NOT EXISTS places (LocationId INTEGER PRIMARY KEY, Location TEXT, Latitude INTEGER, Longitude INTEGER)''')

#fetching lat long details from locations db
cur.execute('''SELECT LocationId FROM places''')
id = [int(rows[0]) for rows in cur.fetchall()]

latlong=[]

for i in id:
    cur.execute('''SELECT * FROM places''')
    latlong=(cur.fetchall())

# print(type(latlong[0][2]))


#executing weather data extraction for all locations in locations db
for i in latlong:

    today = date.today()                            #getting todays' date
    yesterday = today - timedelta(days =1)          #getting yesterdays date
    day_before_yest = today - timedelta(days =2)     #getting day before yesterdays date
    time_day  =''                                   #variable to name the file according to time

    now = datetime.now()                   #getting current time
    current_time = now.strftime("%H")      #changing current time to current hour
    time_day = str(current_time)
    # print(time_day)

    #declaring longitude and latitude for jagdishpur
    id = i[0]
    name = i[1]
    lat = i[2]
    lon = i[3]

    fcast = requests.get('https://api.openweathermap.org/data/2.5/onecall?lat='+lat+
    '&lon='+lon+'&exclude={part}&appid='+api_key, verify= False)

    fcast_api = fcast.json()

    cur.execute('''CREATE TABLE IF NOT EXISTS weather (LocationId INTEGER, Date INTEGER, Time INTEGER, Temp INTEGER, Humidity INTEGER, windspeed INTEGER, feels_like INTEGER, dew_point INTEGER, clouds TEXT, pressure INTEGER, visibility INTEGER, weather_id INTEGER, weather_icon TEXT, weather_main TEXT, weather_desc TEXT, pop INTEGER, PRIMARY KEY(LocationId, Date, Time), FOREIGN KEY(LocationId) REFERENCES places(LocationId))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS weather_hist (LocationId INTEGER, Date INTEGER, Time INTEGER, Temp INTEGER, Humidity INTEGER, windspeed INTEGER, feels_like INTEGER, dew_point INTEGER, clouds TEXT, pressure INTEGER, visibility INTEGER, weather_id INTEGER, weather_icon TEXT, weather_main TEXT, weather_desc TEXT, pop INTEGER, PRIMARY KEY(LocationId, Date, Time), FOREIGN KEY(LocationId) REFERENCES places(LocationId))''')

    for dat in fcast_api['hourly']:
        for fields in dat['weather']:
            weather_d=fields['description']
            weather_id=fields['id']
            weather_icon=fields['icon']
            weather_main=fields['main']

        cur.execute('''INSERT INTO  weather (LocationId, Date, Time, Temp, Humidity, windspeed, feels_like, dew_point, clouds, pressure, visibility,  weather_desc,  weather_id, weather_icon, weather_main, pop)
                VALUES (?, ?, ?, ? , ? , ? , ? , ? , ? , ? , ? ,? ,? ,? ,? ,?)''', (id,(time.strftime('%Y-%m-%d', time.localtime(dat['dt']))), (time.strftime('%H:%M:%S', time.localtime(dat['dt']))), (dat['temp']- 273.15), dat['humidity'], dat['wind_speed'], (dat['feels_like']-273.15), (dat['dew_point']-273.15), dat['clouds'], dat['pressure'], dat['visibility'], weather_d, weather_id, weather_icon, weather_main, dat['pop'] ) )

        cur.execute('''INSERT INTO  weather_hist (LocationId, Date, Time, Temp, Humidity, windspeed, feels_like, dew_point, clouds, pressure, visibility,  weather_desc,  weather_id, weather_icon, weather_main, pop)
                VALUES (?, ?, ?, ? , ? , ? , ? , ? , ? , ? , ? ,? ,? ,? ,? ,?)''', (id,(time.strftime('%Y-%m-%d', time.localtime(dat['dt']))), (time.strftime('%H:%M:%S', time.localtime(dat['dt']))), (dat['temp']- 273.15), dat['humidity'], dat['wind_speed'], (dat['feels_like']-273.15), (dat['dew_point']-273.15), dat['clouds'], dat['pressure'], dat['visibility'], weather_d, weather_id, weather_icon, weather_main, dat['pop'] ) )

        conn.commit()

        cur.execute('''DELETE FROM weather where (JULIANDAY(DATE('now'))-JULIANDAY(Date))>4''')
