-- =====================================================================
-- 02_incremental_batch.sql
--
-- Lands a new batch on gold_employee so the incremental Gold+ model has
-- something to do. Run this BETWEEN two dbt runs.
--
--   500 brand-new employees   -> exercise the merge INSERT path
--   200 existing employees updated -> exercise the merge UPDATE path
--
-- Both carry updated_at after the as-of anchor, so the model's watermark
-- predicate picks them up and reads nothing else out of the large table.
--
-- Re-runnable: each invocation shifts the new-employee id range forward by
-- reading the current max, so you can land several batches in a session.
-- =====================================================================

-- ---------------------------------------------------------------------
-- New hires
-- ---------------------------------------------------------------------

INSERT INTO main.gold_demo.gold_employee
WITH anchor AS (
    SELECT coalesce(max(employee_id), 5100000) + 1 AS start_id FROM main.gold_demo.gold_employee
),
ids AS (
    SELECT a.start_id + r.id AS employee_id
    FROM anchor a
    CROSS JOIN range(0, 500) r
),
assigned AS (
    SELECT
        employee_id,
        1 + pmod(hash(employee_id, 'newdept'), (SELECT count(*) FROM main.gold_demo.gold_department)) AS department_number
    FROM ids
)
SELECT
    a.employee_id,
    element_at(array('Aarav', 'Priya', 'Chen', 'Maria', 'James', 'Fatima', 'Daniel', 'Yuki', 'Omar', 'Sofia', 'Liam', 'Ananya', 'Wei', 'Isabella', 'Noah', 'Zara', 'Ethan', 'Mei', 'Rahul', 'Elena', 'Marcus', 'Nadia', 'Hiroshi', 'Camila', 'Tobias', 'Lakshmi', 'Dmitri', 'Amara', 'Felix', 'Ingrid'), 1 + pmod(hash(a.employee_id, 'fn'), 30))
        || ' '
        || element_at(array('Sharma', 'Okafor', 'Zhang', 'Rodriguez', 'Whitfield', 'Al-Rashid', 'Kowalski', 'Tanaka', 'Haddad', 'Moreau', 'Sullivan', 'Iyer', 'Liu', 'Rossi', 'Andersen', 'Khan', 'Brennan', 'Nakamura', 'Kedia', 'Papadopoulos', 'Volkov', 'Mensah', 'Lindqvist', 'Ferreira', 'Castellanos', 'Nguyen', 'Berhane', 'Fitzgerald', 'Yamamoto', 'Dubois'), 1 + pmod(hash(a.employee_id, 'ln'), 30))
                                                                       AS employee_name,
    a.department_number,
    d.department_name,
    element_at(array('NYC1', 'JRC1', 'CMH1', 'PLN1', 'CHI1', 'TPA1', 'WLM1', 'LDN1', 'BMH1', 'GLA1', 'DUB1', 'MUM1', 'HYD1', 'BLR1', 'SGP1', 'HKG1', 'TOK1', 'SYD1', 'BUE1', 'SAO1'), 1 + pmod(hash(a.employee_id, 'loc'), 20))
                                                                       AS location_code,
    element_at(array('B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'), 1 + pmod(hash(a.employee_id, 'band'), 7))
                                                                       AS salary_band,
    timestampadd(MINUTE, pmod(hash(a.employee_id, 'newts'), 720), TIMESTAMP '2026-08-14 00:00:00')
                                                                       AS updated_at,
    pmod(a.employee_id, 32)                                AS employee_id_bucket
FROM assigned a
JOIN main.gold_demo.gold_department d
    ON a.department_number = d.department_number;

-- ---------------------------------------------------------------------
-- Internal transfers: existing employees change department.
--
-- This is the interesting half. The employee's department_name changes,
-- which means the Gold+ row has to be restated -- exactly the case a
-- naive insert-only incremental gets wrong and a merge gets right.
-- ---------------------------------------------------------------------

MERGE INTO main.gold_demo.gold_employee AS t
USING (
    WITH movers AS (
        SELECT employee_id
        FROM main.gold_demo.gold_employee
        WHERE updated_at < TIMESTAMP '2026-08-14 00:00:00'
        ORDER BY pmod(hash(employee_id, 'move'), 1000000)
        LIMIT 200
    )
    SELECT
        m.employee_id,
        d.department_number AS new_department_number,
        d.department_name   AS new_department_name,
        timestampadd(MINUTE, pmod(hash(m.employee_id, 'movets'), 720), TIMESTAMP '2026-08-14 00:00:00')
            AS new_updated_at
    FROM movers m
    JOIN main.gold_demo.gold_department d
        ON d.department_number =
           1 + pmod(hash(m.employee_id, 'movedept'), (SELECT count(*) FROM main.gold_demo.gold_department))
) AS s
ON t.employee_id = s.employee_id
WHEN MATCHED THEN UPDATE SET
    t.department_number = s.new_department_number,
    t.department_name   = s.new_department_name,
    t.updated_at        = s.new_updated_at;

-- Note: department_number is a partition column, so these updates physically
-- rewrite rows into different partitions. That is realistic, and it is also a
-- concrete illustration of why partitioning on a mutable key hurts -- worth
-- mentioning out loud if the room asks why Liquid Clustering is preferred.
