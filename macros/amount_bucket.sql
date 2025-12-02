{% macro amount_bucket(column_name) %}
    CASE
        WHEN {{ column_name }} < 100 THEN 'small'
        WHEN {{ column_name }} < 1000 THEN 'medium'
        ELSE 'large'
    END
{% endmacro %}