{% macro test_accepted_range(model, column_name, min_value=None, max_value=None) %}

select {{ column_name }}
from {{ model }}
where
    {% if min_value is not none and max_value is not none %}
        {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
    {% elif min_value is not none %}
        {{ column_name }} < {{ min_value }}
    {% elif max_value is not none %}
        {{ column_name }} > {{ max_value }}
    {% else %}
        false
    {% endif %}

{% endmacro %}