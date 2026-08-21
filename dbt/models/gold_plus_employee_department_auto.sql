-- Gold+ join-and-recluster: the defer-the-decision variant.
--
-- CLUSTER BY AUTO. Databricks picks clustering keys from observed query history
-- via predictive optimization. This is the direct answer to "must the modeler
-- decide the physical layout in advance?" -- no.
--
-- Requires dbt-databricks 1.10.0+. Cannot be combined with liquid_clustered_by
-- or partition_by on the same model.

{{ config(
    materialized = 'table',
    auto_liquid_cluster = true,
    enabled = false
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
    on e.department_name = d.department_name
