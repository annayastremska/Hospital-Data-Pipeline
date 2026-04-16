with source as (

    select * from {{ source('hospital_raw', 'procedures') }}

),

renamed as (

    select
        -- keys
        patient                                 as patient_id,
        encounter                               as encounter_id,

        -- timestamps
        cast(start as datetime)                 as procedure_start_at,
        cast(stop  as datetime)                 as procedure_end_at,

        -- procedure attributes
        code                                    as procedure_code,
        description                             as procedure_description,

        -- costs
        cast(base_cost       as numeric)        as base_cost,
        cast(procedure_cost  as numeric)        as procedure_cost,
        cast(medicine_cost   as numeric)        as medicine_cost,

        -- reason
        cast(reasoncode as string)              as reason_code,
        reasondescription                       as reason_description

    from source

)

select * from renamed