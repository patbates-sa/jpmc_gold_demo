-- Gold+ join-and-recluster: the incremental variant.
--
-- The watermark filter sits INSIDE the employee CTE, not after the join, so the
-- predicate pushes down and the run reads only new and changed rows out of the
-- large table rather than scanning it and discarding afterwards.
--
-- merge on employee_id keeps the table correct when an employee transfers
-- department: the Gold+ row is restated rather than duplicated. Run
-- sql/02_incremental_batch.sql between two dbt runs to exercise both paths.

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'employee_id',
    liquid_clustered_by = ['department_name']
) }}

with employee as (
    select * from {{ source('gold', 'gold_employee') }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

department as (
    select * from {{ source('gold', 'gold_department') }}
)

select
    e.employee_id,
    e.employee_name,
    e.department_number,
    e.department_name,
    e.location_code,
    e.salary_band,
    e.updated_at,
    d.department_head,
    d.cost_center,
    d.division
from employee e
join department d
    on e.department_name = d.department_name
