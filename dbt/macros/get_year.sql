-- ============================================================
-- Macro: get_year(column)
-- Cross-adapter safe year extraction
-- ============================================================
{% macro get_year(column) %}
        EXTRACT(YEAR FROM {{ column }})
{% endmacro %}