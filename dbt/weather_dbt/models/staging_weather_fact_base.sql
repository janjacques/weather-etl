{{ config(materialized='table') }}

select
    location_id,
    validdate,
    lat,
    lon,
    t_2m_c,
    msl_pressure_hpa,
    precip_1h_mm,
    wind_speed_10m_ms,
    wind_dir_10m_d,
    weather_symbol_1h_idx
FROM {{ source('staging', 'weather_fact') }}