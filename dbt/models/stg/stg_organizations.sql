with source as (

    select * from {{ ref('organizations') }}

),

renamed as (

    select
        id                  as organization_id,
        name                as organization_name,
        address,
        city,
        state,
        zip,
        lat                 as latitude,
        lon                 as longitude

    from source

)

select * from renamed