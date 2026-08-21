-- Gold+ join-and-recluster: the table-materialized variant.
--
-- Laid out for how CONSUMERS read this table, not for how the sources ingest.
-- department_name is the clustering key because consumers filter on it -- the
-- fact that it is also the join key is a coincidence of this example, not a rule.

{{ config(
    materialized = 'table',
    liquid_clustered_by = ['department_name']
) }}

with employee as (
    select * from {{ source('gold', 'gold_employee') }}
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
    on e.department_name = d.department_name   -- join key need NOT be a layout key
