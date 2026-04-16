-- ============================================================
-- fact_executive_summary
-- ============================================================
-- Objective:
--   Single-row-per-year executive dashboard:
--   visit volume, revenue, payer coverage, average costs, and
--   patient count.  Each row = one calendar year.
--
-- Key business questions answered:
--   • Are visit volumes growing year-over-year?
--   • What is total and average revenue per encounter?
--   • What share of costs is covered by payers vs. patients?
--   • How many unique patients were active each year?
-- ============================================================

{{ config(
    materialized = 'table',
    tags         = ['mart', 'executive']
) }}

with encounters as (
    select
        {{ get_year('encounter_start_at') }}        as encounter_year,
        encounter_id,
        patient_id,
        encounter_class,
        total_claim_cost,
        payer_coverage,
        base_encounter_cost,
        (total_claim_cost - payer_coverage)          as patient_out_of_pocket
    from {{ ref('stg_encounters') }}
),

yearly as (
    select
        encounter_year,

        -- Volume
        count(encounter_id)                                      as total_encounters,
        count(distinct patient_id)                               as unique_patients,

        -- Revenue
        round(sum(total_claim_cost), 2)                          as total_revenue,
        round(avg(total_claim_cost), 2)                          as avg_revenue_per_encounter,

        -- Payer vs patient split
        round(sum(payer_coverage), 2)                            as total_payer_coverage,
        round(sum(patient_out_of_pocket), 2)                     as total_patient_out_of_pocket,
        {{ pct_of_total('sum(payer_coverage)', 'sum(total_claim_cost)') }}
                                                                 as pct_covered_by_payer,

        -- Base cost (before procedures)
        round(avg(base_encounter_cost), 2)                       as avg_base_encounter_cost,

        -- Zero-coverage encounters
        count(case when payer_coverage = 0 then 1 end)           as zero_coverage_encounters,
        {{ pct_of_total(
            'count(case when payer_coverage = 0 then 1 end)',
            'count(encounter_id)'
        ) }}                                                     as pct_zero_coverage

    from encounters
    group by encounter_year
)

select
    encounter_year,
    total_encounters,
    unique_patients,
    total_revenue,
    avg_revenue_per_encounter,
    total_payer_coverage,
    total_patient_out_of_pocket,
    pct_covered_by_payer,
    avg_base_encounter_cost,
    zero_coverage_encounters,
    pct_zero_coverage,

    -- YoY revenue growth (requires window function)
    round(
        100.0 * (total_revenue - lag(total_revenue) over (order by encounter_year))
              / nullif(lag(total_revenue) over (order by encounter_year), 0),
        2
    )                                                            as revenue_growth_pct_yoy,

    round(
        100.0 * (total_encounters - lag(total_encounters) over (order by encounter_year))
              / nullif(lag(total_encounters) over (order by encounter_year), 0),
        2
    )                                                            as encounter_growth_pct_yoy

from yearly
order by encounter_year desc