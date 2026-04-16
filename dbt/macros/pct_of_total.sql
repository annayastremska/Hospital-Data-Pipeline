-- ============================================================
-- Macro: pct_of_total(numerator, denominator)
-- Returns a rounded percentage, NULL-safe
-- ============================================================
{% macro pct_of_total(numerator, denominator) %}
    ROUND(
        100.0 * {{ numerator }} / NULLIF({{ denominator }}, 0),
        2
    )
{% endmacro %}