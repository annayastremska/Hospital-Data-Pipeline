with source as (

    select * from {{ source('hospital_raw', 'vitals') }}

),

renamed as (

    select
        -- keys
        patient_id,
        encounter_id,

        -- encounter context (kept for now; reconcile with encounters at mart layer)
        encounter_class,

        -- timestamps
        cast(timestamp as datetime)             as recorded_at,

        -- vitals
        cast(heart_rate        as int64)        as heart_rate,
        cast(oxygen_level      as int64)        as oxygen_level,
        cast(systolic_bp       as int64)        as systolic_bp,
        cast(diastolic_bp      as int64)        as diastolic_bp,
        cast(temperature_c     as float64)      as temperature_c,
        cast(respiratory_rate  as int64)        as respiratory_rate

    from source

)

select * from renamed