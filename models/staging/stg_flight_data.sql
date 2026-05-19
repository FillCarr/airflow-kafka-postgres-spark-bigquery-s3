WITH source AS (

    SELECT * FROM {{ source('stg', 'raw_flight_data') }}

),

renamed AS (

    SELECT
        icao24,
        time_position,
        latitude,
        longitude,
        velocity,
        heading_deviation,
        geo_altitude,
        vertical_rate,
        baro_altitude
    FROM source

)

SELECT * FROM renamed
