-- ============================================================
-- mart_readmissions
-- ============================================================
-- Objective:
--   Identify patients readmitted within 30 days of a previous
--   encounter and surface common characteristics (age group,
--   organ system, encounter class) to detect avoidable
--   readmission patterns.
--
-- Clinical & financial relevance:
--   • Readmissions within 30 days are a standard quality
--     metric — payers (incl. CMS) penalise hospitals for them.
--   • Identifying the dominant organ systems and patient
--     profiles enables targeted discharge protocols.
--
-- Granularity:
--   Part A — patient-level flag (was_readmitted within 30d)
--   Part B — summary profile by organ_system + age_bucket
-- ============================================================

{{ config(
    materialized = 'table',
    tags         = ['mart', 'clinical', 'quality']
) }}

-- ── Part A: flag each encounter for readmission ───────────────
with encounters as (
    select
        e.encounter_id,
        e.patient_id,
        e.encounter_start_at,
        e.encounter_end_at,
        e.encounter_class,
        e.organ_system,
        e.total_claim_cost,

        -- Patient demographics
        p.race,
        p.gender,
        {{ get_year('p.birth_date') }} as birth_year,
        -- Age at time of this encounter
        DATE_DIFF(cast(e.encounter_start_at as date), p.birth_date, YEAR) as age_at_encounter

    from {{ ref('stg_encounters') }} e
    left join {{ ref('stg_patients') }} p
        on e.patient_id = p.patient_id
),

-- Self-join: find the *next* encounter for same patient
with_next as (
    select
        curr.encounter_id,
        curr.patient_id,
        curr.encounter_start_at                                as curr_start,
        curr.encounter_end_at                                  as curr_stop,
        curr.encounter_class                                   as curr_class,
        curr.organ_system                                      as curr_organ_system,
        curr.total_claim_cost,
        curr.race,
        curr.gender,
        curr.age_at_encounter,
        {{ age_bucket('curr.age_at_encounter') }}              as age_bucket,

        -- Next encounter details
        min(nxt.encounter_start_at)                            as next_encounter_start,
        min(nxt.encounter_id)                                  as next_encounter_id

    from encounters curr
    left join encounters nxt
        on  curr.patient_id        = nxt.patient_id
        and nxt.encounter_start_at > curr.encounter_end_at  -- strictly after current ends
    group by
        curr.encounter_id, curr.patient_id, curr.encounter_start_at,
        curr.encounter_end_at, curr.encounter_class, curr.organ_system,
        curr.total_claim_cost, curr.race, curr.gender,
        curr.age_at_encounter
),

flagged as (
    select
        *,
        case
            when next_encounter_start is not null
             and next_encounter_start <= DATETIME_ADD(curr_stop, INTERVAL 30 DAY)
            then true else false
        end as readmitted_within_30d,

        -- Days until readmission (null if not readmitted within 30d)
        case
            when next_encounter_start is not null
             and next_encounter_start <= DATETIME_ADD(curr_stop, INTERVAL 30 DAY)
            then DATE_DIFF(cast(next_encounter_start as date), cast(curr_stop as date), DAY)
        end as days_to_readmission

    from with_next
)

-- ── Part B: aggregate profile of readmitted encounters ────────
select
    curr_organ_system                                          as organ_system,
    curr_class                                                 as encounter_class,
    age_bucket,
    race,
    gender,

    count(*)                                                   as total_encounters,
    sum(cast(readmitted_within_30d as int64))                  as readmitted_count,
    {{ pct_of_total(
        'sum(cast(readmitted_within_30d as int64))',
        'count(*)'
    ) }}                                                       as readmission_rate_pct,

    round(avg(days_to_readmission), 1)                        as avg_days_to_readmission,
    round(avg(total_claim_cost), 2)                           as avg_claim_cost

from flagged
where curr_organ_system is not null
group by
    curr_organ_system, curr_class, age_bucket, race, gender
having
    sum(cast(readmitted_within_30d as int64)) > 0
order by
    readmitted_count desc