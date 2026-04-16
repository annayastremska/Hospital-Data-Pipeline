with source as (

    select * from {{ source('hospital_raw', 'encounters') }}

),

renamed as (

    select
        -- keys
        id                                          as encounter_id,
        patient                                     as patient_id,
        organization                                as organization_id,
        payer                                       as payer_id,

        -- timestamps
        cast(start as datetime)                     as encounter_start_at,
        cast(stop  as datetime)                     as encounter_end_at,

        -- encounter attributes
        encounterclass                              as encounter_class,
        code                                        as encounter_code,
        description                                 as encounter_description,

        -- costs
        cast(base_encounter_cost  as numeric)       as base_encounter_cost,
        cast(total_claim_cost     as numeric)       as total_claim_cost,
        cast(payer_coverage       as numeric)       as payer_coverage,
        cast(procedures_total     as numeric)       as procedures_total,

        -- reason
        cast(reasoncode as string)                  as reason_code,
        reasondescription                           as reason_description,

        -- metadata
    NULLIF(organ_system, '') as organ_system
    from source

)

select * from renamed