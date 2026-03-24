{% set important_aircrafts = ['CN1', 'CR2', '763']%}

select
    {% for aircraft in important_aircrafts -%}
    sum (case when aircraft_id = '{{ aircraft }}' then 1 else 0 end) as flights_{{ aircraft }}
        {%- if not loop.last %}, {% endif %}
    {% endfor %}
from 
    {{ ref('fct_flights') }}