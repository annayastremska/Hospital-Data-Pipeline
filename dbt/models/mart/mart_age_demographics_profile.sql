-- ============================================================
-- mart_age_demographics_profile
-- ============================================================
-- Objective:
--   Show how encounter volume, revenue, organ system mix, and
--   encounter class vary by patient age group and gender.
--
-- Business & clinical questions:
--   • Which age groups consume the most healthcare resources?
--   • Do older patients skew toward inpatient / emergency?
--   • Are there gender differences in organ system utilisation?
--   • Where should preventive care programmes be targeted?
--
-- Granularity: one row per age_bucket × gender × organ_system
-- ============================================================

{{ config(
    materialized = 'table',
    tags         = ['mart', 'clinical', 'demographics']
) }}

with base as (
    select
        e.encounter_id,
        e.patient_id,
        e.encounter_class,
        e.organ_system,
        e.total_claim_cost,
        e.payer_coverage,
        e.encounter_start_at,

        p.gender,
        p.race,

        -- Age at encounter
        DATE_DIFF(
            cast(e.encounter_start_at as date),
            p.birth_date,
            YEAR
        )                                            as age_at_encounter

    from {{ ref('stg_encounters') }} e
    left join {{ ref('stg_patients') }} p
        on e.patient_id = p.patient_id
    where p.birth_date is not null
),

bucketed as (
    select
        *,
        {{ age_bucket('age_at_encounter') }}         as age_bucket
    from base
)

select
    age_bucket,
    gender,
    organ_system,

    count(encounter_id)                              as encounter_count,
    count(distinct patient_id)                       as unique_patients,

    round(sum(total_claim_cost), 2)                  as total_revenue,
    round(avg(total_claim_cost), 2)                  as avg_claim_cost,

    -- Payer coverage ratio for this demographic segment
    {{ pct_of_total('sum(payer_coverage)', 'sum(total_claim_cost)') }}
                                                     as pct_covered_by_payer,

    -- Acuity proxy: share of inpatient + emergency
    {{ pct_of_total(
        "count(case when encounter_class in ('inpatient','emergency') then 1 end)",
        'count(encounter_id)'
    ) }}                                             as pct_high_acuity_encounters,

    round(avg(age_at_encounter), 1)                  as avg_age

from bucketed
where organ_system is not null
  and gender       is not null
group by age_bucket, gender, organ_system
order by encounter_count desc