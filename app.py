from http.client import NO_CONTENT
from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json
import numpy as np
import pandas as pd
app = Flask(__name__) 

@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

#2
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    station = get_trip_id(trip_id, conn)
    return station.to_json()

#3
@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

#4
@app.route('/stations/average_footprint') 
def average_footprint():
    conn = make_connection()
    avg_footprint = get_avg_footprint(conn)
    return avg_footprint.to_json()

@app.route('/trips/average_duration') 
def average_duration_trips():
    conn = make_connection()
    avg_duration = get_avg_duration_trips(conn)
    return avg_duration.to_json()

#5
@app.route('/stations/total_station/<council_district>')
def total_station_by_council_district(council_district):
    conn = make_connection()
    count_station = get_total_station_by_council_district(council_district, conn)
    return count_station.to_json()

@app.route('/trips/average_duration/<bike_id>')
def average_duration_by_bike_id(bike_id):
    conn = make_connection()
    avg_duration = get_avg_duration_by_bike_id(bike_id, conn)
    return avg_duration.to_json()

#6
@app.route('/stations/', methods=['POST'])
def top_n_duration_by_route():
    input_data = request.get_json()
    station_name = input_data['station_name']
    top_n = input_data['limit']

    conn = make_connection()
    selected_data = get_avg_duration_by_route(station_name, conn)

    result = selected_data.groupby(['Start Station', 'End Station']).mean('Duration').reset_index()\
    .sort_values(by='Duration', ascending=False)\
    .head(top_n)
    return result.to_json()

#MISC
@app.route('/json') 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

############################################FUNCTIONS############################################

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

conn = make_connection()


#2
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

#3
def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations VALUES {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips VALUES {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

#4
def get_avg_footprint(conn):
    query = f"""SELECT AVG(footprint_length) AS 'Rata-rata Footprint Length', 
    AVG(footprint_width) 'Rata-rata Footprint Width' FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_avg_duration_trips(conn):
    query = f"""SELECT AVG(duration_minutes) AS 'Rata-rata Durasi' FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result 

#5
def get_total_station_by_council_district(council_district, conn):
    query = f"""SELECT COUNT(station_id) AS 'Total Stasiun' FROM stations
    WHERE council_district = {council_district}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_avg_duration_by_bike_id(bike_id, conn):
    query = f"""SELECT AVG(duration_minutes) AS 'Rata-rata Durasi' FROM trips
    WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

#6
def get_avg_duration_by_route(station_name, conn):
    query = f"""SELECT s1.name AS 'Start Station', s2.name AS 'End Station', t.duration_minutes AS 'Duration'
    FROM trips AS t JOIN stations AS s1 ON t.start_station_id = s1.station_id 
    JOIN stations AS s2 ON t.end_station_id = s2.station_id 
    WHERE s1.name LIKE '%{station_name}%' OR s2.name LIKE '%{station_name}%'
    """
    result = pd.read_sql_query(query, conn)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5000)
