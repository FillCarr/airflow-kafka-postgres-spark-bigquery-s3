from kafka import KafkaConsumer
import json
import psycopg
import datetime as dt

def main ():
    pass
def get_dem_data():
    pass
def bounding_box():    
    pass

def get_flight_weather():
    buffer_fw = []
    buffer_size = 50
    if (len(buffer_fw) < buffer_size):
        flight_data = KafkaConsumer('flight_data')
        weather_data = KafkaConsumer('weather_data')
        for flight, weather in zip(flight_data, weather_data):
                    flight_json = json.loads(flight.value)
                    weather_json = json.loads(weather.value)
                    merged_json = flight_json | weather_json
                    buffer_fw.append(merged_json)
                    print(f"{len(buffer_fw)}/100" )
                    if (len(buffer_fw) == buffer_size):
                        connect_to_db(buffer_fw)
                        buffer_fw.clear() 


def connect_to_db(buffer_fw):
    with psycopg.connect(dbname="geeks", user='postgres', password='root', host='localhost', port= '5432') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS flight_weather 
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
            cur.executemany("INSERT INTO flight_weather (icao24, on_ground, geo_altitude, latitude, longitude, velocity, vertical_rate, baro_altitude, last_contact, time_position, location, temperature, humidity, temperature_2m, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high, pressure_msl, surface_pressure, precipitation, wind_speed_10m, wind_speed_80m, wind_speed_120m, wind_speed_180m, wind_direction_10m, wind_direction_80m, wind_direction_120m, wind_direction_180m, wind_gusts_10m, temperature_80m, temperature_180m, temperature_120m) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", buffer_fw)

if __name__ == "__main__":
    get_flight_weather()