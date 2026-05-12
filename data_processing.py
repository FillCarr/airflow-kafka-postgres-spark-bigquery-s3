from kafka import KafkaConsumer
from multiprocessing import Process
import os
import json
import datetime as dt
import psycopg
import threading
import time

def main ():
    buffer_size=100
    buffer_flights = []
    buffer_weather = []
    t1 = threading.Thread(target=get_flight_data)
    t2 = threading.Thread(target=get_weather_data)
    t3 = threading.Thread(target=connect_to_db)

    t1.start()
    t2.start()
    if len(buffer_flights) or len(buffer_weather) == buffer_size:
        t3.start()

def get_flight_data():
    flight_data = KafkaConsumer('flight_data')
    print(f"consuming flight data... @ {dt.datetime.now().hour}:{dt.datetime.now().minute}")
    for message in flight_data:
        flight_json = json.loads(message.value)
        buffer_flights.append(flight_json)
        return flight_json
    
def get_weather_data():
    weather_data= KafkaConsumer('weather_data')
    print(f"consuming weather data... @ {dt.datetime.now().hour}:{dt.datetime.now().minute}")
    for message in weather_data:
        weather = json.loads(message.value)
        buffer_weather.append(weather)
        return weather        

def connect_to_db():
        with psycopg.connect(dbname="geeks", user='postgres', password='root', host='localhost', port= '5432') as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS flight_data 
                    (
                        icao24 TEXT,
                        on_ground BOOLEAN,
                        geo_altitude FLOAT,
                        latitude FLOAT,
                        longitude FLOAT,
                        velocity FLOAT,
                        vertical_rate FLOAT,
                        baro_altitude FLOAT,
                        last_contact TIMESTAMP,
                        time_position TIMESTAMP
                    );"""
                    )
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data 
                    (
                        location TEXT,
                        temperature FLOAT,
                        humidity FLOAT,
                        temperature_2m FLOAT,
                        cloud_cover FLOAT,
                        cloud_cover_low FLOAT,
                        cloud_cover_mid FLOAT,
                        cloud_cover_high FLOAT,
                        pressure_msl FLOAT,
                        surface_pressure FLOAT,
                        precipitation FLOAT,
                        wind_speed_10m FLOAT,
                        wind_speed_80m FLOAT,
                        wind_speed_120m FLOAT,
                        wind_speed_180m FLOAT,
                        wind_direction_10m FLOAT,
                        wind_direction_80m FLOAT,
                        wind_direction_120m FLOAT,
                        wind_direction_180m FLOAT,
                        wind_gusts_10m FLOAT,
                        temperature_80m FLOAT,
                        temperature_180m FLOAT,
                        temperature_120m FLOAT
                    );
                    """
                )
                cur.executemany("INSERT INTO flight_data (icao24, on_ground, geo_altitude, latitude, longitude, velocity, vertical_rate, baro_altitude, last_contact, time_position) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", buffer_flights)
                cur.executemany("INSERT INTO weather_data (location, temperature, humidity, temperature_2m, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high, pressure_msl, surface_pressure, precipitation, wind_speed_10m, wind_speed_80m, wind_speed_120m, wind_speed_180m, wind_direction_10m, wind_direction_80m, wind_direction_120m, wind_direction_180m, wind_gusts_10m, temperature_80m, temperature_180m, temperature_120m) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", buffer_weather)
                conn.commit()
                buffer_flights.clear()
                buffer_weather.clear()

if __name__ == "__main__":
    main()