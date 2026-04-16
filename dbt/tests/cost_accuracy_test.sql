-- Test: base_encounter_cost + procedures_total should equal total_claim_cost
-- Returns rows where the difference exceeds the tolerance (0.01).
-- Any returned rows = test failure.

with encounters as (

    select
        encounter_id,
        base_encounter_cost,
        procedures_total,
        total_claim_cost,
        round(base_encounter_cost + procedures_total, 2)    as expected_total,
        round(total_claim_cost, 2)                          as actual_total
    from {{ ref('stg_encounters') }}
    where total_claim_cost is not null
      and base_encounter_cost is not null
      and procedures_total is not null

)

select
    encounter_id,
    base_encounter_cost,
    procedures_total,
    expected_total,
    actual_total,
    round(abs(actual_total - expected_total), 4)            as discrepancy
from encounters
where abs(actual_total - expected_total) > 0.01