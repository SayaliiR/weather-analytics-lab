-- Singular test: degree days can never be negative, and a day cannot need
-- both heating and cooling. Returns failing rows (test passes when empty).
select weather_id, heating_degree_days, cooling_degree_days
from {{ ref('weather_metrics') }}
where heating_degree_days < 0
   or cooling_degree_days < 0
   or (heating_degree_days > 0 and cooling_degree_days > 0)
