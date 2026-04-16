-- ============================================================
-- Macro: age_bucket(age_column)
-- Standardised age grouping used across multiple marts
-- ============================================================
{% macro age_bucket(age_column) %}
    CASE
        WHEN {{ age_column }} < 18  THEN '<18'
        WHEN {{ age_column }} < 35  THEN '18-34'
        WHEN {{ age_column }} < 50  THEN '35-49'
        WHEN {{ age_column }} < 65  THEN '50-64'
        WHEN {{ age_column }} < 80  THEN '65-79'
        ELSE '80+'
    END
{% endmacro %}