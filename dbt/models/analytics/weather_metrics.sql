-- Analytics: per-city daily weather with rolling and derived metrics
with weather as (
    select * from {{ ref('stg_weather') }}
),

base as (
    select
        *,
        -- dry day: less than 1 mm of precipitation
        case when precipitation_sum < 1 then 1 else 0 end as is_dry_day,
        -- degree days: how far the daily mean is below / above the base temperature
        -- (null when the day's mean temperature is missing)
        case when temp_mean is not null
             then greatest({{ var('degree_day_base_c') }} - temp_mean, 0) end as heating_degree_days,
        case when temp_mean is not null
             then greatest(temp_mean - {{ var('degree_day_base_c') }}, 0) end as cooling_degree_days,
        
        case
            when weather_code is null                        then 'Unknown'
            when weather_code = 0                            then 'Clear'
            when weather_code in (1, 2)                      then 'Partly Cloudy'
            when weather_code = 3                            then 'Overcast'
            when weather_code in (45, 48)                    then 'Fog'
            when weather_code in (51, 53, 55, 56, 57)        then 'Drizzle'
            when weather_code in (61, 63, 65, 66, 67, 80, 81, 82) then 'Rain'
            when weather_code in (71, 73, 75, 77, 85, 86)    then 'Snow'
            when weather_code in (95, 96, 99)                then 'Thunderstorm'
            else 'Unknown'
        end                                                           as weather_condition,
        -- city baseline: average daily mean temperature over observed (non-forecast) days
        avg(case when not is_forecast then temp_mean end)
            over (partition by city)                                  as city_avg_temp
    from weather
),

metrics as (
    select
        *,
        round(avg(temp_mean) over (
            partition by city order by date
            rows between 6 preceding and current row), 2)            as temp_mean_7d_avg,
        round(avg(temp_mean) over (
            partition by city order by date
            rows between 29 preceding and current row), 2)           as temp_mean_30d_avg,
        round(temp_mean - city_avg_temp, 2)                           as temp_anomaly,
        round(sum(precipitation_sum) over (
            partition by city order by date
            rows between 6 preceding and current row), 2)            as rainfall_7d_total,
        -- gaps-and-islands: consecutive dry days share the same group number
        row_number() over (partition by city order by date)
          - row_number() over (partition by city, is_dry_day order by date) as dry_group
    from base
)

select
    weather_id,
    city,
    latitude,
    longitude,
    date,
    is_forecast,
    temp_max,
    temp_min,
    temp_mean,
    temp_range,
    precipitation_sum,
    wind_speed_max,
    weather_code,
    weather_condition,
    temp_mean_7d_avg,
    temp_mean_30d_avg,
    round(city_avg_temp, 2)                                          as city_avg_temp,
    temp_anomaly,
    rainfall_7d_total,
    is_dry_day,
    case
        when is_dry_day = 1
        then row_number() over (partition by city, is_dry_day, dry_group order by date)
        else 0
    end                                                              as dry_spell_length,
    round(heating_degree_days, 2)                                    as heating_degree_days,
    round(cooling_degree_days, 2)                                    as cooling_degree_days
from metrics
