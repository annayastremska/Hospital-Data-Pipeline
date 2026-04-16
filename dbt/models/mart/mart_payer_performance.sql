-- ============================================================
-- mart_payer_performance
-- ============================================================
-- Objective:
--   Understand financial exposure by payer: which payers
--   cover the most, which leave the most cost to patients,
--   and how many encounters had zero coverage.
--
-- Business questions answered:
--   • How many encounters had zero payer coverage, and what
--     % of total does that represent?
--   • What is the average total claim cost by payer?
--   • Which payer relationships are most profitable?
--   • Are uninsured volumes trending up or down?
--
-- Granularity: one row per payer
-- ============================================================

{{ config(
    materialized = 'table',
    tags         = ['mart', 'finance', 'payers']
) }}

with encounters as (
    select
        e.encounter_id,
        e.payer_id,
        p.payer_name,
        e.total_claim_cost,
        e.payer_coverage,
        e.base_encounter_cost,
        (e.total_claim_cost - e.payer_coverage) as patient_responsibility,
        case when e.payer_coverage = 0 then 1 else 0 end as is_zero_coverage
    from {{ ref('stg_encounters') }} e
    left join {{ ref('stg_payers') }} p
        on e.payer_id = p.payer_id
),

totals as (
    select count(encounter_id) as grand_total_encounters
    from encounters
)

select
    e.payer_name,

    -- Volume
    count(e.encounter_id)                            as total_encounters,
    {{ pct_of_total('count(e.encounter_id)', 'max(t.grand_total_encounters)') }}
                                                     as pct_of_all_encounters,

    -- Financials
    round(sum(e.total_claim_cost), 2)                as total_billed,
    round(avg(e.total_claim_cost), 2)                as avg_claim_cost,
    round(sum(e.payer_coverage), 2)                  as total_payer_coverage,
    round(avg(e.payer_coverage), 2)                  as avg_payer_coverage,
    round(sum(e.patient_responsibility), 2)          as total_patient_responsibility,
    round(avg(e.patient_responsibility), 2)          as avg_patient_responsibility,

    -- Coverage rate
    {{ pct_of_total('sum(e.payer_coverage)', 'sum(e.total_claim_cost)') }}
                                                     as pct_cost_covered_by_payer,

    -- Zero coverage
    sum(e.is_zero_coverage)                          as zero_coverage_encounters,
    {{ pct_of_total('sum(e.is_zero_coverage)', 'count(e.encounter_id)') }}
                                                     as pct_zero_coverage

from encounters e
cross join totals t
group by e.payer_name
order by total_encounters desc