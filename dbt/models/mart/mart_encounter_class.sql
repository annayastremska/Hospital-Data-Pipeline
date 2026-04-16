-- ============================================================
-- mart_encounter_class_
-- ============================================================
-- Objective:
--   For every calendar year, show the percentage of total
--   encounters that belonged to each encounter class
--   (ambulatory, outpatient, wellness, urgentcare,
--    emergency, inpatient).
--
-- Business question:
--   "Has the share of emergency/inpatient visits changed
--    over time?  Are we seeing a shift toward preventive
--    (wellness) care?"  Trend shifts inform staffing,
--    bed capacity, and billing strategy.
--
-- Granularity: one row per year × encounter_class
-- ============================================================

{{ config(
    materialized = 'table',
    tags         = ['mart', 'executive', 'operations']
) }}

with encounters as (
    select
        {{ get_year('encounter_start_at') }} as encounter_year,
        encounter_class,
        encounter_id,
        total_claim_cost
    from {{ ref('stg_encounters') }}
),

yearly_totals as (
    select
        encounter_year,
        count(encounter_id)      as year_total_encounters,
        sum(total_claim_cost)    as year_total_revenue
    from encounters
    group by encounter_year
),

by_class as (
    select
        e.encounter_year,
        e.encounter_class,
        count(e.encounter_id)             as encounter_count,
        round(sum(e.total_claim_cost), 2) as class_revenue,
        round(avg(e.total_claim_cost), 2) as avg_claim_cost
    from encounters e
    group by e.encounter_year, e.encounter_class
)

select
    bc.encounter_year,
    bc.encounter_class,
    bc.encounter_count,
    bc.class_revenue,
    bc.avg_claim_cost,

    {{ pct_of_total('bc.encounter_count', 'yt.year_total_encounters') }}
                                         as pct_of_encounters,
    {{ pct_of_total('bc.class_revenue',  'yt.year_total_revenue') }}
                                         as pct_of_revenue

from by_class bc
join yearly_totals yt
    on bc.encounter_year = yt.encounter_year
order by bc.encounter_year desc, bc.encounter_class