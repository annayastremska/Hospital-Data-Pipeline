-- ============================================================
-- mart_monthly_procedure_costs  (INCREMENTAL)
-- ============================================================
-- Objective:
--   Track monthly procedure cost trends incrementally.
--   This is the incremental mart — new procedure rows appended
--   each month are merged without rebuilding the whole table.
--
-- Why incremental here?
--   Procedures are the highest-volume source table
--   and grow with every encounter.  Full-refresh every run is
--   wasteful; incremental on start date keeps things fast.
--
-- Business questions:
--   • Which procedures are driving cost growth month-over-month?
--   • Are medicine costs a meaningful share of procedure costs?
--
-- Granularity: one row per year-month × procedure_description
-- ============================================================

{{
    config(
        materialized     = 'incremental',
        unique_key       = ['year_month', 'procedure_description'],
        on_schema_change = 'append_new_columns',
        tags             = ['mart', 'finance', 'incremental']
    )
}}

with procedures as (
    select
        pr.encounter_id,
        pr.patient_id,
        pr.procedure_description,
        pr.procedure_cost,
        pr.medicine_cost,
        pr.procedure_start_at,

        -- Month grain
        cast(date_trunc(pr.procedure_start_at, month) as date) as year_month,


    from {{ ref('stg_procedures') }} pr

    -- ── Incremental filter ───────────────────────────────────
    {% if is_incremental() %}
    where pr.procedure_start_at > (
        select max(procedure_start_at_max) from {{ this }}
    )
    {% endif %}
),

aggregated as (
    select
        year_month,
        procedure_description,

        count(*)                                 as procedure_count,
        count(distinct patient_id)               as unique_patients,
        round(sum(procedure_cost), 2)            as total_procedure_cost,
        round(avg(procedure_cost), 2)            as avg_procedure_cost,
        round(sum(medicine_cost), 2)             as total_medicine_cost,
        round(avg(medicine_cost), 2)             as avg_medicine_cost,
        round(
            sum(procedure_cost) + sum(medicine_cost),
            2
        )                                        as total_cost_incl_medicine,

        -- Track the watermark for the incremental filter
        max(procedure_start_at)                  as procedure_start_at_max

    from procedures
    group by year_month, procedure_description
    order by year_month desc
)

select * from aggregated