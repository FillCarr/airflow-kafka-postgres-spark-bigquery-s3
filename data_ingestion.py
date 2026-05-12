from opensky_api import OpenSkyApi, TokenManager
from kafka import KafkaProducer
import json
import datetime as dt
import openmeteo_requests
import requests_cache
from retry_requests import retry

def main():
    reduce_requests()

def reduce_requests():
    print(f"start_time @ {dt.datetime.now().hour}:{dt.datetime.now().minute}")
    while True:
        if (dt.datetime.now().second == 0):
            flight_dict = raw_flight_data()
            print(f"flight_info_gathered @ {dt.datetime.now().hour}:{dt.datetime.now().minute}")
            weather_dict = raw_weather_data(flight_dict)
            print(f"weather_and_flight_data_sent @ {dt.datetime.now().hour}:{dt.datetime.now().minute}")
            data_to_kafka(flight_dict, weather_dict)
            

def raw_flight_data():
    with OpenSkyApi(token_manager=TokenManager.from_json_file("credentials.json")) as api:
        states = api.get_states(bbox=(40.95, 42.05, -73.75, -71.78))
        for s in states.states:
            flight_dict = {
                "icao24": s.icao24,
                "velocity": s.velocity,
                "vertical_rate": s.vertical_rate,
                "baro_altitude": s.baro_altitude,
                "last_contact": s.last_contact,
                "time_position": s.time_position,
                "on_ground": s.on_ground,
                "geo_altitude": s.geo_altitude,
                "latitude": s.latitude,
                "longitude": s.longitude,
            }          
    return flight_dict

#https://open-meteo.com/en/docs/historical-forecast-api?hourly=
def raw_weather_data(flight_dict):
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": flight_dict["latitude"],
        "longitude": flight_dict["longitude"],
        "current": ["temperature_2m", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "pressure_msl", "surface_pressure", "precipitation", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", "temperature_180m", "temperature_120m"],
    }
    responses = openmeteo.weather_api(url, params = params)
    response = responses[0]
    #https://pypi.org/project/openmeteo-requests/
    current = response.Current()

    print(f"plane: {flight_dict['icao24']}")
    print(f"coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"land_elevation: {response.Elevation()} m asl")
    weather_dict = {
        "temperature_2m": current.Variables(0).Value(),
        "cloud_cover": current.Variables(1).Value(),
        "cloud_cover_low": current.Variables(2).Value(),
        "cloud_cover_mid": current.Variables(3).Value(),
        "cloud_cover_high": current.Variables(4).Value(),
        "pressure_msl": current.Variables(5).Value(),
        "surface_pressure": current.Variables(6).Value(),
        "precipitation": current.Variables(7).Value(),
        "wind_speed_10m": current.Variables(8).Value(),
        "wind_speed_80m": current.Variables(9).Value(),
        "wind_speed_120m": current.Variables(10).Value(),
        "wind_speed_180m": current.Variables(11).Value(),
        "wind_direction_10m": current.Variables(12).Value(),
        "wind_direction_80m": current.Variables(13).Value(),
        "wind_direction_120m": current.Variables(14).Value(),
        "wind_direction_180m": current.Variables(15).Value(),
        "wind_gusts_10m": current.Variables(16).Value(),
        "temperature_80m": current.Variables(17).Value(),
        "temperature_180m": current.Variables(18).Value(),
        "temperature_120m": current.Variables(19).Value(),
    }    
    return weather_dict

def data_to_kafka(flight_dict, weather_dict):
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(f'flight_data', flight_dict)
    producer.send(f'weather_data', weather_dict)
    producer.flush()
    print(f"kafka_send_time: {dt.datetime.now().hour}:{dt.datetime.now().minute}")
    return producer

if __name__ == "__main__":
    main()