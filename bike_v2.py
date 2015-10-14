import requests
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3 as lite
import time
from dateutil.parser import parse
import collections
import datetime

## API call
r = requests.get('http://www.citibikenyc.com/stations/json')
#
## Create dataframe
df = json_normalize(r.json()['stationBeanList'])
#
## DB connection
con = lite.connect('citi_bike_4.db')
cur = con.cursor()
#
## create tables
with con:
    cur.execute('CREATE TABLE IF NOT EXISTS citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
    
    sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
#    
    # wrapped this in data check to see if there is data in table, if so don't run
    populated_check = cur.execute('SELECT rowid FROM citibike_reference WHERE rowid = 1;').fetchone()
    print "POPULATED CHECK", populated_check    
    #try:
    if populated_check is None:
        for station in r.json()['stationBeanList']:
            cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))
    #except:
    else:   
         print "Column data has already been populated"
        
# extract column from dataframe and put in list
station_ids = df['id'].tolist()
# add '_' to station name + ad ddata type
station_ids = ['_' + str(x) + ' INT' for x in station_ids]
#
## create the available bikes table
with con:
    cur.execute("CREATE TABLE IF NOT EXISTS available_bikes ( execution_time INT, " + ", ".join(station_ids) + ");")

def update_available_bikes():    
    # API call
    r = requests.get('http://www.citibikenyc.com/stations/json')    
    # take the string and parse to py datetime
    exec_time = parse(r.json()['executionTime'])
    print exec_time
    print exec_time.strftime('%s')
    
    # create entry for exeuction time by inserting into db
    with con:
        cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
    
    # iterate through stations in list
    id_bikes = collections.defaultdict(int) # defaultdict to store available bikes by station
    
    # loop through stations in station list
    for station in r.json()['stationBeanList']:
        id_bikes[station['id']] = station['availableBikes']
    
    # iterate through defaultdict to update values in db
    with con:
        for k, v in id_bikes.iteritems():
            cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
 
times_ran = 0
while times_ran < 61:
    update_available_bikes()
    times_ran += 1
    print "Script has run %r / 60 times" % times_ran
    time.sleep(60)
  
df2 = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time", con, index_col="execution_time")

hour_change = collections.defaultdict(int) # currently empty. will become dictionary where k is list of station ids and v is abs change over time

for col in df2.columns:
    station_vals = df2[col].tolist()
    #print "STATION VALS: ", station_vals
    station_id = col[1:] #trim the "_"
    #print "STATION IDS: ", station_id
    station_change = 0
    #pdb.set_trace()    
    for k,v in enumerate(station_vals): 
        # how can we k,v enumerate a list?
        # my understanding is station_vals is just list of items in a column. is it dict of 327 lists?
        # is k the index of the list? > YES
        if k < len(station_vals) - 1: # why the if here?
            station_change += abs(station_vals[k] - station_vals[k+1]) # why the +1?

        
    hour_change[int(station_id)] = station_change # convert station id back to int
    
def keywithmaxval(d):
    # create a list of the dict's keys and vals
    v = list(d.values())
    k = list(d.keys())
    # return key with max value
    return k[v.index(max(v))]

# assign the max key to max_station
max_station = keywithmaxval(hour_change)
print "MAX STATION = ",max_station

# get station info

cur.execute("SELECT stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()
print "DATA: ",data
print "The most active station is id %s at latitude: %r longitude: %r " % data
print "With " + str(hour_change[max_station]) + " bicycles coming and going in the hour between " + datetime.datetime.fromtimestamp(int(df2.index[0])).strftime('%Y-%m-%dT%H:%M:%S') + " and " + datetime.datetime.fromtimestamp(int(df2.index[-1])).strftime('%Y-%m-%dT%H:%M:%S')
