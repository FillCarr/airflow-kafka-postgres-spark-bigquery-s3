WITH flight_base AS (

    SELECT
        fd.icao24,
        fd.time_position,
        fd.latitude,
        fd.longitude,
        fd.velocity,
        fd.heading_deviation,
        fd.geo_altitude,
        fd.vertical_rate,
        fd.baro_altitude,
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS dbt_time
    FROM {{ ref('stg_flight_data') }} fd

),

weather_base AS (

    SELECT
        wd.latitude,
        wd.longitude,
        wd.temperature,
        wd.humidity,
        wd.temperature_2m,
        wd.cloud_cover,
        wd.cloud_cover_low,
        wd.cloud_cover_mid,
        wd.cloud_cover_high,
        wd.pressure_msl,
        wd.surface_pressure,
        wd.precipitation,
        wd.wind_speed_10m,
        wd.wind_speed_80m,
        wd.wind_speed_120m,
        wd.wind_speed_180m,
        wd.wind_direction_10m,
        wd.wind_direction_80m,
        wd.wind_direction_120m,
        wd.wind_direction_180m,
        wd.wind_gusts_10m,
        wd.temperature_80m,
        wd.temperature_120m,
        wd.temperature_180m
    FROM {{ ref('stg_weather_data') }} wd

),

joined_data AS (

    SELECT
        fb.icao24,
        fb.time_position,
        fb.latitude,
        fb.longitude,
        fb.velocity,
        fb.heading_deviation,
        fb.geo_altitude,
        fb.vertical_rate,
        fb.baro_altitude,
        wb.temperature,
        wb.humidity,
        wb.temperature_2m,
        wb.cloud_cover,
        wb.cloud_cover_low,
        wb.cloud_cover_mid,
        wb.cloud_cover_high,
        wb.pressure_msl,
        wb.surface_pressure,
        wb.precipitation,
        wb.wind_speed_10m,
        wb.wind_speed_80m,
        wb.wind_speed_120m,
        wb.wind_speed_180m,
        wb.wind_direction_10m,
        wb.wind_direction_80m,
        wb.wind_direction_120m,
        wb.wind_direction_180m,
        wb.wind_gusts_10m,
        wb.temperature_80m,
        wb.temperature_120m,
        wb.temperature_180m,
        fb.dbt_time
    FROM flight_base fb
    LEFT JOIN weather_base wb
        ON fb.latitude = wb.latitude
        AND fb.longitude = wb.longitude

),

rolling_metrics AS (

    SELECT
        icao24,
        time_position,
        latitude,
        longitude,
        velocity,
        heading_deviation,
        geo_altitude,
        vertical_rate,
        baro_altitude,
        temperature,
        humidity,
        cloud_cover,
        pressure_msl,
        wind_speed_10m,
        wind_speed_80m,
        wind_speed_120m,
        wind_speed_180m,
        STDDEV(heading_deviation) OVER (PARTITION BY icao24) AS heading_deviation_stddev,
        AVG(velocity)             OVER (PARTITION BY icao24) AS avg_velocity,
        VARIANCE(wind_speed_10m)  OVER (PARTITION BY icao24) AS variance_wind_speed_10m,
        VARIANCE(wind_speed_80m)  OVER (PARTITION BY icao24) AS variance_wind_speed_80m,
        VARIANCE(wind_speed_120m) OVER (PARTITION BY icao24) AS variance_wind_speed_120m,
        VARIANCE(wind_speed_180m) OVER (PARTITION BY icao24) AS variance_wind_speed_180m,
        dbt_time
    FROM joined_data

),

anomaly_detection AS (

    SELECT *
    FROM rolling_metrics
    WHERE
        heading_deviation_stddev > (AVG(heading_deviation_stddev) OVER () + 2 * STDDEV(heading_deviation_stddev) OVER ())
        OR avg_velocity           > (AVG(avg_velocity)           OVER () + 2 * STDDEV(avg_velocity)           OVER ())
        OR variance_wind_speed_10m > (AVG(variance_wind_speed_10m) OVER () + 2 * STDDEV(variance_wind_speed_10m) OVER ())

)

SELECT * FROM anomaly_detection
