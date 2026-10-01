{% snapshot snapshot_weather_daily %}

{{
  config(
    target_schema='snapshot',
    unique_key='weather_id',
    strategy='check',
    check_cols=['temp_max', 'temp_min', 'temp_mean', 'precipitation_sum', 'wind_speed_max', 'weather_code', 'is_forecast'],
    invalidate_hard_deletes=False
  )
}}

"""Tracks how each city-day changes over time, e.g. a forecast later replaced by the
observed value. Rows that age out of the API's 60-day window stay in the snapshot,
so it also keeps history beyond what the raw table holds."""
select
    city || '_' || to_char(date, 'YYYY-MM-DD') as weather_id,
    *
from {{ source('raw', 'weather_daily') }}

{% endsnapshot %}
