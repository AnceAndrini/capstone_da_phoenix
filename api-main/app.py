  
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
@app.route('/stations')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()
  
@app.route('/trips')    
def route_all_trips():
    conn = make_connection()
    trips= get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

@app.route('/stations/SumOfStatusStation')
def route_SumOfStatusStation():
    conn = make_connection()
    station = get_SumOfStatusStation(conn)
    return station.to_json()

@app.route('/trips/BikeOfStartStation/<start_time>')
def route_BikeOfStartStation(start_time):
    conn = make_connection()
    trip = get_BikeOfStartStation(start_time, conn)
    return trip.to_json()

@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result


@app.route('/trips/Averagetime', methods=['POST']) 

def route_AverageTime():
    input_data = request.get_json(force=True) # Get the input as dictionary
    
    specified_date = input_data['period'] # Select specific items (period) from the dictionary (the value will be "2015-08")

    conn = make_connection()

    result = selected_data(specified_date, conn).groupby('start_station_id').agg({
        'bikeid' : 'count', 
        'duration_minutes' : 'mean'
    })

    return result.to_json()

    
########## FUNCTION ############
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def selected_data(specified_date,conn):
    query = f"""SELECT * FROM trips WHERE start_time LIKE '{specified_date}%'"""
    selected_data = pd.read_sql_query(query, conn)
    result = pd.read_sql_query(query, conn)
    return result

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result



def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query = f"""SELECT * FROM trips LIMIT 5"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_BikeOfStartStation(start_time, conn):
    query = f"""SELECT start_station_name, COUNT (bikeid) AS Jumlah
                FROM trips 
                WHERE start_time LIKE '{start_time}%'
                GROUP BY start_station_name
                ORDER BY Jumlah DESC"""
    result = pd.read_sql_query(query, conn)
    return result

    
def get_SumOfStatusStation(conn):
    query = f"""SELECT Status,COUNT (station_id) AS TotalStation FROM stations GROUP BY status ORDER BY TotalStation DESC"""
    result = pd.read_sql_query(query, conn)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5000)
