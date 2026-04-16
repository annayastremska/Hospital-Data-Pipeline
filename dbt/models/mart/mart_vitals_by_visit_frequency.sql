-- ============================================================
-- mart_vitals_by_visit_frequency
-- ============================================================
-- Objective:
--   Compare average vital signs between the most-frequent
--   and least-frequent visitors (within 2022, the year for
--   which we have vitals data).
--
-- Clinical question:
--   "Do patients who visit most often show measurably worse
--    vitals than occasional visitors?"  If yes, frequent
--    visitors are likely the sicker, higher-acuity cohort
--    that needs dedicated care pathways.
--
-- Bucketing logic (fixed thresholds):
--   low_frequency    = 1-2 visits
--   medium_frequency = 3-5 visits
--   high_frequency   = 6+ visits
--
-- Granularity:
--   One row per frequency_bucket showing average vitals
--   and visit counts.
-- ============================================================

{{ config(
    materialized = 'table',
    tags         = ['mart', 'clinical', 'vitals']
) }}

-- Step 1: count 2022 visits per patient
with patient_visit_counts as (
    select
        patient_id,
        count(encounter_id) as visit_count_2022
    from {{ ref('stg_encounters') }}
    where {{ get_year('encounter_start_at') }} = 2022
    group by patient_id
),

-- Step 2: label patients by fixed visit thresholds
frequency_buckets as (
    select
        patient_id,
        visit_count_2022,
        case
            when visit_count_2022 >= 6 then 'high_frequency'
            when visit_count_2022 >= 3 then 'medium_frequency'
            else                            'low_frequency'
        end as frequency_bucket
    from patient_visit_counts
),

-- Step 3: join vitals to frequency bucket

vitals_with_bucket as (
    select
        fb.patient_id,
        fb.frequency_bucket,
        fb.visit_count_2022,
        v.encounter_id,
        v.heart_rate,
        v.oxygen_level,
        v.systolic_bp,
        v.diastolic_bp,
        v.temperature_c,
        v.respiratory_rate
    from {{ ref('stg_vitals') }} v
    join frequency_buckets fb
        on v.patient_id = fb.patient_id
)

-- Step 4: aggregate vitals per bucket
select
    frequency_bucket,
    count(*)                             as vital_measurement_rows,
    count(distinct encounter_id)         as encounters_with_vitals,
    count(distinct patient_id)           as patient_count,

    -- Heart
    round(avg(heart_rate), 1)            as avg_heart_rate,
    round(min(heart_rate), 1)            as min_heart_rate,
    round(max(heart_rate), 1)            as max_heart_rate,

    -- Blood pressure
    round(avg(systolic_bp), 1)           as avg_systolic_bp,
    round(avg(diastolic_bp), 1)          as avg_diastolic_bp,

    -- Oxygen
    round(avg(oxygen_level), 1)          as avg_oxygen_level,

    -- Temperature
    round(avg(temperature_c), 2)         as avg_temp_c,

    -- Respiratory
    round(avg(respiratory_rate), 1)      as avg_respiratory_rate,

    -- Average visits for context
    round(avg(visit_count_2022), 1)      as avg_visits_in_bucket

from vitals_with_bucket
group by frequency_bucket
order by
    case frequency_bucket
        when 'high_frequency'   then 1
        when 'medium_frequency' then 2
        else 3
    end