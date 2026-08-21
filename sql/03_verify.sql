-- =====================================================================
-- 03_verify.sql
--
-- Run after generation, before the session. Every check below should pass
-- or the demo misleads. Read the comments -- each one states the expected
-- answer, so a wrong result is obvious without cross-referencing.
-- =====================================================================

-- 1. Row counts. Expect 5,000,000 employees, 24 departments.
SELECT 'gold_employee' AS table_name, count(*) AS rows FROM main.gold_demo.gold_employee
UNION ALL
SELECT 'gold_department', count(*) FROM main.gold_demo.gold_department;

-- 2. The join key must be unique on the small side, or the join is not
--    many-to-one and the row count fans out. Expect zero rows.
SELECT department_name, count(*) AS n
FROM main.gold_demo.gold_department
GROUP BY department_name
HAVING count(*) > 1;

-- 3. Referential integrity on the join key. Expect zero rows.
--    If this returns anything, the Gold+ join silently drops employees.
SELECT e.department_name, count(*) AS orphaned_employees
FROM main.gold_demo.gold_employee e
LEFT JOIN main.gold_demo.gold_department d
    ON e.department_name = d.department_name
WHERE d.department_name IS NULL
GROUP BY e.department_name;

-- 4. Partition layout. Confirms the join key is NOT a partition column --
--    the premise of the entire pattern. Look at the partitionColumns field.
DESCRIBE DETAIL main.gold_demo.gold_employee;
DESCRIBE DETAIL main.gold_demo.gold_department;

-- 5. Partition count and rows per partition on the large side.
--    Watch for over-partitioning: if rows-per-partition is in the tens,
--    say so in the room rather than letting someone notice it. That is the
--    small-file problem Hive-style partitioning causes, and it is the
--    argument for Liquid Clustering on the Gold+ table.
SELECT
    count(*)                                              AS partitions,
    sum(rows)                                             AS total_rows,
    cast(avg(rows) AS INT)                                AS avg_rows_per_partition,
    min(rows)                                             AS min_rows,
    max(rows)                                             AS max_rows
FROM (
    SELECT department_number, employee_id_bucket, count(*) AS rows
    FROM main.gold_demo.gold_employee
    GROUP BY department_number, employee_id_bucket
);

-- 6. Skew check. The first three departments should hold roughly 30% of
--    employees between them. Real skew is what makes AQE's runtime
--    coalescing and split behaviour visible rather than theoretical.
SELECT
    department_number,
    department_name,
    count(*)                                                        AS employees,
    round(100.0 * count(*) / (SELECT count(*) FROM main.gold_demo.gold_employee), 2)    AS pct_of_total
FROM main.gold_demo.gold_employee
GROUP BY department_number, department_name
ORDER BY employees DESC
LIMIT 10;

-- 7. Watermark distribution. Confirms updated_at spans 90 days
--    so the incremental predicate has a meaningful boundary to cut on.
SELECT
    min(updated_at) AS earliest,
    max(updated_at) AS latest,
    count(DISTINCT date(updated_at)) AS distinct_days
FROM main.gold_demo.gold_employee;

-- 8. The join plan. Confirm a BroadcastHashJoin on the small side, and that
--    no partition pruning is claimed on department_name. This is the single
--    most useful thing to show on screen: it proves the reframe rather than
--    asserting it.
EXPLAIN FORMATTED
SELECT e.employee_id, e.department_name, d.cost_center, d.division
FROM main.gold_demo.gold_employee e
JOIN main.gold_demo.gold_department d
    ON e.department_name = d.department_name;

-- 9. After the dbt model has been built: confirm the clustering keys landed.
--    Look for clusteringColumns = [department_name] and note that
--    partitionColumns is empty -- the Gold+ table is laid out for reads,
--    not for ingest.
-- DESCRIBE DETAIL main.gold_demo.gold_plus_employee_department;

-- 10. After the dbt model has been built: row count must equal
--     gold_employee exactly. A many-to-one join must neither fan out
--     nor drop rows, and this is the one-line proof.
-- SELECT
--     (SELECT count(*) FROM main.gold_demo.gold_employee)  AS employee_rows,
--     (SELECT count(*) FROM main.gold_demo.gold_plus_employee_department) AS gold_plus_rows;
