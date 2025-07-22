{{ config(materialized='table') }}


with fact as (
    select
        location_id,
        cast(validdate as date) as observation_date,
        t_2m_c,
        msl_pressure_hpa,
        precip_1h_mm,
        wind_speed_10m_ms,
        wind_dir_10m_d
    from {{ref('staging_weather_fact_base')}}
),

agg as (
    select
        location_id,
        observation_date,
        avg(t_2m_c) as avg_t_2m_c,
        avg(msl_pressure_hpa) as avg_msl_pressure_hpa,
        sum(precip_1h_mm) as total_precip_1h_mm,   -- summing makes sense for precipitation
        avg(wind_speed_10m_ms) as avg_wind_speed_10m_ms,
        avg(wind_dir_10m_d) as avg_wind_dir_10m_d
    from fact
    group by location_id, observation_date
),
final as (
    select
        a.location_id,
        l.city,
        l.country,
        a.observation_date,
        a.avg_t_2m_c,
        a.avg_msl_pressure_hpa,
        a.total_precip_1h_mm,
        a.avg_wind_speed_10m_ms,
        a.avg_wind_dir_10m_d
    from agg a
    left join {{ source('staging', 'location_dim') }} l
        on a.location_id = l.location_id
)

select * from final