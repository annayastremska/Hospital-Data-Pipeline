-- ============================================================
-- mart_organ_system_investment
-- ============================================================
-- Objective:
--   Identify which organ systems drive the most encounter
--   volume and revenue — the primary signal for equipment
--   and staffing investment decisions.
--
-- Business question:
--   "We think most patients come in for cardiac / vascular
--    issues. Should we invest in more equipment for those?"
--   This mart quantifies it so leadership can act on data,
--   not intuition.
--
-- Granularity: one row per organ_system per year
-- ============================================================

{{ config(
    materialized = 'table',
    tags         = ['mart', 'clinical', 'investment']
) }}

with encounters as (
    select
        {{ get_year('encounter_start_at') }}  as encounter_year,
        organ_system,
        encounter_id,
        patient_id,
        total_claim_cost,
        payer_coverage,
        encounter_class
    from {{ ref('stg_encounters') }}
    where organ_system is not null
),

yearly_totals as (
    select
        encounter_year,
        count(encounter_id) as year_total_encounters
    from encounters
    group by encounter_year
),

by_system as (
    select
        e.encounter_year,
        e.organ_system,

        count(e.encounter_id)                        as encounter_count,
        count(distinct e.patient_id)                 as unique_patients,
        round(sum(e.total_claim_cost), 2)            as total_revenue,
        round(avg(e.total_claim_cost), 2)            as avg_claim_cost,
        round(avg(e.payer_coverage), 2)              as avg_payer_coverage,

        -- most common encounter class per organ system
        APPROX_TOP_COUNT(e.encounter_class, 1)[OFFSET(0)].value
                                                     as dominant_encounter_class

    from encounters e
    group by e.encounter_year, e.organ_system
)

select
    bs.encounter_year,
    bs.organ_system,
    bs.encounter_count,
    bs.unique_patients,
    bs.total_revenue,
    bs.avg_claim_cost,
    bs.avg_payer_coverage,
    bs.dominant_encounter_class,

    -- Share of total encounters that year → priority signal
    {{ pct_of_total('bs.encounter_count', 'yt.year_total_encounters') }}
                                                     as pct_of_year_encounters,

    -- Rank by volume so dashboards can sort easily
    rank() over (
        partition by bs.encounter_year
        order by bs.encounter_count desc
    )                                                as volume_rank

from by_system bs
join yearly_totals yt
    on bs.encounter_year = yt.encounter_year
order by bs.encounter_year, volume_rank