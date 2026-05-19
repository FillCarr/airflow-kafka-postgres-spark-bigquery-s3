WITH source AS (

    SELECT * FROM {{ source('stg', 'raw_weather_data') }}

),

renamed AS (

    SELECT
        latitude,
        longitude,
        temperature,
        humidity,
        temperature_2m,
        cloud_cover,
        cloud_cover_low,
        cloud_cover_mid,
        cloud_cover_high,
        pressure_msl,
        surface_pressure,
        precipitation,
        wind_speed_10m,
        wind_speed_80m,
        wind_speed_120m,
        wind_speed_180m,
        wind_direction_10m,
        wind_direction_80m,
        wind_direction_120m,
        wind_direction_180m,
        wind_gusts_10m,
        temperature_80m,
        temperature_120m,
        temperature_180m
    FROM source

)

SELECT * FROM renamed
