with source as (

    select * from {{ source('hospital_raw', 'patients') }}

),

renamed as (

    select
        -- keys
        id                                      as patient_id,

        -- dates
        cast(birthdate  as date)                as birth_date,
        cast(deathdate  as date)                as death_date,

        -- name
        prefix,
        first                                   as first_name,
        last                                    as last_name,
        suffix,
        maiden                                  as maiden_name,

        -- demographics
        marital                                 as marital_status,
        race,
        ethnicity,
        gender,

        -- location
        birthplace                              as birth_place,
        address,
        city,
        state,
        county,
        cast(zip as string)                     as zip,
        cast(lat as float64)                    as latitude,
        cast(lon as float64)                    as longitude

    from source

)

select * from renamed