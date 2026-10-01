-- Staging: clean, typed view of the raw table (ephemeral, compiled as a CTE)
with source as (
    select * from {{ source('raw', 'weather_daily') }}
)
select
    city || '_' || to_char(date, 'YYYY-MM-DD') as weather_id,
    city,
    latitude,
    longitude,
    date,
    temp_max,
    temp_min,
    temp_mean,
    temp_max - temp_min                        as temp_range,
    coalesce(precipitation_sum, 0)             as precipitation_sum,
    wind_speed_max,
    weather_code,
    is_forecast
from source
where city is not null
  and date is not null
